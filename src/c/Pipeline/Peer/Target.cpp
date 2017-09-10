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



#include "Pipeline/Peer/Target.h"
#include "Pipeline/Peer/Protocol.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>

namespace Pipeline {
namespace Peer {

Target::Address::Address(const char *host, const char *port)
      : host(host), port(port) { }

void Target::shutdown() {
  manager.deregister_close_and_clear(fd);
}

void Target::start_connection() {
  if (fd != -1) {
    return;
  }

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

Target::Target(const Address           &address,
                     Epoll::Manager    &manager,
                     Paxos::Legislator &legislator,
               const NodeName          &node_name)
  : address(address),
    manager(manager),
    legislator(legislator),
    node_name(node_name) {
  start_connection();
}

void Target::handle_readable() {
  fprintf(stderr, "%s: TODO\n",
                  __PRETTY_FUNCTION__);
  abort();
}

void Target::handle_writeable() {
  if (fd == -1) {
    return;
  }

  if (!sent_handshake) {
    printf("%s (fd=%d): connected\n", __PRETTY_FUNCTION__, fd);
    assert(fd != -1);
    Protocol::send_handshake(fd, node_name);
    manager.modify_handler(fd, this, EPOLLIN);
    sent_handshake = true;
    return;
  }

  fprintf(stderr, "%s: TODO\n",
                  __PRETTY_FUNCTION__);
  abort();
}

void Target::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
  assert(fd == -1);
}

}}
