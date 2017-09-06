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



#include "Pipeline/Client/Socket.h"

namespace Pipeline {
namespace Client {

void Socket::shutdown() {
  pipe.close_write_end();
  manager.deregister_close_and_clear(fd);
}

Socket::Socket
       (Epoll::Manager                  &manager,
        Paxos::Legislator               &legislator,
        const NodeName                  &node_name,
        const Paxos::Value::StreamName   stream,
        const int                        fd)
  : manager         (manager),
    legislator      (legislator),
    node_name       (node_name),
    stream          (stream),
    pipe            (manager, node_name, stream),
    fd              (fd) {

  manager.register_handler(fd, this, EPOLLIN);
}

Socket::~Socket() {
  shutdown();
}

bool Socket::is_shutdown() const {
  return fd == -1;
}

void Socket::handle_readable() {
  fprintf(stderr, "%s (fd=%d): TODO\n",
                  __PRETTY_FUNCTION__, fd);
  abort();
}

void Socket::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  abort();
}

void Socket::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
}

}
}
