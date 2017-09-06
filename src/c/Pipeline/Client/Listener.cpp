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



#include "Pipeline/Client/Listener.h"

#include <sys/socket.h>
#include <netdb.h>

namespace Pipeline {
namespace Client {

void Listener::handle_accept(int client_fd) {
  printf("TODO\n");
  abort();
}

Listener::Listener(Epoll::Manager    &manager,
                   Paxos::Legislator &legislator,
                   const std::string &cluster_name,
                   const Paxos::NodeId node_id,
                   const char        *port)
  : AbstractListener(manager, port),
    legislator(legislator),
    cluster_name(cluster_name),
    node_id(node_id) {}

}
}
