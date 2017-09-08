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



#ifndef COMMAND_LISTENER_H
#define COMMAND_LISTENER_H

#include "Epoll.h"
#include "Paxos/Legislator.h"
#include "Pipeline/AbstractListener.h"
#include "Pipeline/NodeName.h"

#include <memory>
#include <sstream>

using NodeName = Pipeline::NodeName;

namespace Command {

class Socket : public Epoll::Handler {
  Socket           (const Socket&) = delete; // no copying
  Socket &operator=(const Socket&) = delete; // no assignment

  Epoll::Manager        &manager;
  Paxos::Legislator     &legislator;
  const NodeName        &node_name;
  int                    fd = -1;

  void shutdown() {
    manager.deregister_close_and_clear(fd);
  }

public:
  Socket(Epoll::Manager    &manager,
         Paxos::Legislator &legislator,
   const NodeName          &node_name,
   const int                fd)
    : manager(manager),
      legislator(legislator),
      node_name(node_name),
      fd(fd) {

      manager.register_handler(fd, this, EPOLLIN);
  }

  ~Socket() { shutdown(); }

  bool is_shutdown() const { return fd == -1; }

#define COMMAND_BUF_SIZE 1024

  void handle_readable() override {
    if (is_shutdown()) { return; }

    char command_buf[COMMAND_BUF_SIZE];

    ssize_t read_result = read(fd, command_buf, COMMAND_BUF_SIZE);
    if (read_result == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: read() failed\n", __PRETTY_FUNCTION__);
      shutdown();
    } else if (read_result == 0) {
      fprintf(stderr, "%s: EOF\n", __PRETTY_FUNCTION__);
      shutdown();
    } else {
      assert(read_result > 0);
      size_t bytes_read = read_result;
      if (COMMAND_BUF_SIZE <= bytes_read) {
        fprintf(stderr, "%s: command too large\n", __PRETTY_FUNCTION__);
        shutdown();
      } else {
        command_buf[bytes_read] = '\0';

        std::basic_istringstream<char> command(command_buf);
        std::basic_ostringstream<char> response;

        std::string word;
        command >> word;

        if (word == "stat") {
          response << "cluster: " << node_name.cluster << std::endl
                   << legislator << std::endl;
        } else if (word == "inc" || word == "dec"
                || word == "mul" || word == "div") {

          Paxos::Value value;
          if (word == "inc") {
            value.type = Paxos::Value::Type::reconfiguration_inc;
            command >> value.payload.reconfiguration.subject;
          } else if (word == "dec") {
            value.type = Paxos::Value::Type::reconfiguration_dec;
            command >> value.payload.reconfiguration.subject;
          } else if (word == "mul") {
            value.type = Paxos::Value::Type::reconfiguration_mul;
            uint32_t factor;
            command >> factor;
            value.payload.reconfiguration.factor = factor;
          } else if (word == "div") {
            value.type = Paxos::Value::Type::reconfiguration_div;
            uint32_t factor;
            command >> factor;
            value.payload.reconfiguration.factor = factor;
          }
          std::string expect_eof;
          command >> expect_eof;

          if (expect_eof == "EOF") {
            response << "OK proposing reconfiguration: " << value << std::endl;
            legislator.activate_slots(value, 1);
          } else {
            response << "expected '" << word << " <NUM> EOF'" << std::endl;
          }
        } else if (word == "abdicate") {
          Paxos::NodeId new_leader;
          std::string expect_eof;
          command >> new_leader >> expect_eof;
          if (expect_eof == "EOF") {
            response << "OK abdicating to " << new_leader << std::endl;
            legislator.abdicate_to(new_leader);
          } else {
            response << "expected '" << word << " <NUM> EOF'" << std::endl;
          }
        } else if (word == "unsafe-stage-coup") {
          response << "OK unsafely staging a coup" << std::endl;
          legislator.unsafely_stage_coup();
        } else {
          response << "unknown command '" << word << "'" << std::endl;
        }

        response << "EOF" << std::endl;

        std::string response_string = response.str();
        ssize_t write_result = write(fd, response_string.c_str(), (size_t)(response_string.length()));
        if (write_result == -1) {
          perror(__PRETTY_FUNCTION__);
          fprintf(stderr, "%s: write() failed\n", __PRETTY_FUNCTION__);
        }
        shutdown();
      }
    }
  }

  void handle_writeable() override {
    fprintf(stderr, "%s (fd=%d): unexpected\n", __PRETTY_FUNCTION__, fd);
    shutdown();
  }

  void handle_error(const uint32_t events) {
    fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                    __PRETTY_FUNCTION__, fd, events);
    shutdown();
  }
};

class Listener : public Pipeline::AbstractListener {
  Listener           (const Listener&) = delete; // no copying
  Listener &operator=(const Listener&) = delete; // no assignment

  Paxos::Legislator     &legislator;
  const NodeName        &node_name;
  std::vector<std::unique_ptr<Socket>> sockets;

protected:
  void handle_accept(int client_fd) override {
    sockets.erase(std::remove_if(
      sockets.begin(),
      sockets.end(),
      [](const std::unique_ptr<Socket> &c) {
        return c->is_shutdown(); }),
      sockets.end());

    sockets.push_back(std::move(std::unique_ptr<Socket>
      (new Socket(manager, legislator, node_name, client_fd))));
  }

public:
  Listener(Epoll::Manager    &manager,
           Paxos::Legislator &legislator,
           const NodeName    &node_name,
           const char        *port)
    : AbstractListener(manager, port),
      legislator(legislator),
      node_name(node_name) {}
};

}

#endif // ndef COMMAND_LISTENER_H

