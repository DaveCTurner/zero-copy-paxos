/*

    Copyright 2017 David Turner

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

*/



#ifndef COMMAND_REGISTRATION_H
#define COMMAND_REGISTRATION_H

#include "Epoll.h"
#include "Pipeline/NodeName.h"
#include "Pipeline/Peer/Protocol.h"

#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>

namespace Command {

class Registration {

public:

  class Address {
    public:
      const std::string host;
      const std::string port;
      Address(const char *host, const char *port)
        : host(host), port(port) {}
  };

private:

  class Target : public Epoll::Handler {
      const Address        &address;
            Epoll::Manager &manager;
            int             fd = -1;
            std::string    &cluster;
            Paxos::NodeId  &node;
            bool            sent_request = false;

      void shutdown() {
        manager.deregister_close_and_clear(fd);
        sent_request = false;
      }

    public:
      Target(const Address  &address,
             Epoll::Manager &manager,
             std::string    &cluster,
             Paxos::NodeId  &node)
        : address(address),
          manager(manager),
          cluster(cluster),
          node(node) {}

      ~Target() {
        shutdown();
      }

      void handle_readable () override {
        if (fd == -1) { return; }

#define RESPONSE_BUFFER_SIZE 1024
        char response_buffer[RESPONSE_BUFFER_SIZE];
        ssize_t read_result = read(fd, response_buffer, RESPONSE_BUFFER_SIZE);
        if (read_result == -1) {
          perror(__PRETTY_FUNCTION__);
          fprintf(stderr, "%s: read() failed\n", __PRETTY_FUNCTION__);
        } else if (read_result == 0) {
          fprintf(stderr, "%s: EOF\n", __PRETTY_FUNCTION__);
        } else {
          assert(0 <= read_result);
          size_t bytes_read = read_result;
          if (RESPONSE_BUFFER_SIZE <= bytes_read) {
            fprintf(stderr, "%s: overflow\n", __PRETTY_FUNCTION__);
          } else {
            std::basic_istringstream<char> response(response_buffer);
            std::string expect_ok, expect_cluster, new_cluster, expect_node, expect_eof;
            Paxos::NodeId new_node;
            response >> expect_ok >> expect_cluster >> new_cluster
                     >> expect_node >> new_node >> expect_eof;
            if (expect_ok == "OK" && expect_cluster == "cluster"
                && expect_node == "node" && expect_eof == "EOF"
                && new_cluster.length() == CLUSTER_ID_LENGTH
                && new_node > 0) {

              cluster.assign(new_cluster);
              node = new_node;

              std::cout << __PRETTY_FUNCTION__
                  << ": registered as " << new_cluster << "." << new_node << std::endl;

            } else {
              fprintf(stderr, "%s: unexpected response\n", __PRETTY_FUNCTION__);
            }
          }
        }
        shutdown();
      }

      void handle_writeable() override {
        if (fd == -1) { return; }
        if (sent_request) { return; }

        sent_request = true;
        manager.modify_handler(fd, this, EPOLLIN);

        ssize_t write_result = write(fd, "new\n", 4);
        if (write_result == -1) {
          perror(__PRETTY_FUNCTION__);
          fprintf(stderr, "%s: write() failed\n", __PRETTY_FUNCTION__);
          shutdown();
        } else if (write_result != 4) {
          fprintf(stderr, "%s: write() only wrote %ld of 4 bytes\n",
                  __PRETTY_FUNCTION__, write_result);
          shutdown();
        }
      }

      void handle_error(const uint32_t events) {
        fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                        __PRETTY_FUNCTION__, fd, events);
        shutdown();
      }

      void retry() {
        if (fd != -1) { return; }

        struct addrinfo hints;
        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        struct addrinfo *remote_addrinfo;
        int getaddrinfo_result
          = getaddrinfo(address.host.c_str(),
                        address.port.c_str(),
                        &hints, &remote_addrinfo);
        if (getaddrinfo_result != 0) {
          fprintf(stderr, "%s: getaddrinfo(remote) failed: %s\n",
            __PRETTY_FUNCTION__,
            gai_strerror(getaddrinfo_result));
          return;
        }

        for (struct addrinfo *r = remote_addrinfo; r != NULL; r = r->ai_next) {
          fd = socket(r->ai_family,
                      r->ai_socktype | SOCK_NONBLOCK,
                      r->ai_protocol);
          if (fd == -1) {
            perror(__PRETTY_FUNCTION__);
            fprintf(stderr, "%s: socket() failed\n", __PRETTY_FUNCTION__);
            continue;
          }

          int connect_result = connect(fd, r->ai_addr, r->ai_addrlen);
          if (connect_result == 0) {
            handle_writeable();
            break;
          } else {
            assert(connect_result == -1);
            if (errno == EINPROGRESS) {
#ifndef NTRACE
              printf("%s: connect() returned EINPROGRESS\n", __PRETTY_FUNCTION__);
#endif // ndef NTRACE
              manager.register_handler(fd, this, EPOLLOUT);
              break;
            } else {
              perror(__PRETTY_FUNCTION__);
              fprintf(stderr, "%s: connect() failed\n", __PRETTY_FUNCTION__);
              close(fd);
              fd = -1;
              continue;
            }
          }
        }

        freeaddrinfo(remote_addrinfo);
      }

      bool is_shutdown() const { return fd == -1; }
  };

  std::vector<Target> targets;
  Paxos::NodeId &node;
  Epoll::Manager manager;

  Registration(std::string &cluster, Paxos::NodeId &node,
               const std::vector<Address> &addresses)
      : node(node),
        manager() {

    assert(!addresses.empty());

    for (const auto &address : addresses) {
      targets.push_back(Target(address, manager, cluster, node));
    }
  }

  void go() {
    while (node == 0) {
      for (auto &target : targets) {
        if (target.is_shutdown()) {
          target.retry();
        }
      }
      manager.wait(1000);
    }
  }

public:

  static void get_node_name(std::string &cluster, Paxos::NodeId &node,
                            const std::vector<Address> &addresses) {

    if (addresses.empty()) {
      std::ifstream uuid_file("/proc/sys/kernel/random/uuid");
      std::string uuid;
      uuid_file >> uuid;
      cluster.assign(uuid);
      node = 1;
    } else {
      node = 0;
      Registration r(cluster, node, addresses);
      r.go();
    }
  }
};

}

#endif // ndef COMMAND_REGISTRATION_H

