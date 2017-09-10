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

namespace Pipeline {
namespace Peer {

Target::Address::Address(const char *host, const char *port)
      : host(host), port(port) { }

void Target::shutdown() {
  manager.deregister_close_and_clear(fd);
}

Target::Target(const Address           &address,
                     Epoll::Manager    &manager,
                     Paxos::Legislator &legislator,
               const NodeName          &node_name)
  : address(address),
    manager(manager),
    legislator(legislator),
    node_name(node_name) {}

void Target::handle_readable() {
  fprintf(stderr, "%s: TODO\n",
                  __PRETTY_FUNCTION__);
  abort();
}

void Target::handle_writeable() {
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
