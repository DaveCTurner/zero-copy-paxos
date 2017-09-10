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



#include "Pipeline/Peer/Listener.h"

#include <sys/socket.h>
#include <netdb.h>
#include <string.h>

namespace Pipeline {
namespace Peer {

void Listener::handle_accept(int client_fd) {
  peer_sockets.erase(std::remove_if(
    peer_sockets.begin(),
    peer_sockets.end(),
    [](const std::unique_ptr<Socket> &c) {
      return c->is_shutdown(); }),
    peer_sockets.end());

  peer_sockets.push_back(std::move(std::unique_ptr<Socket>
    (new Socket(manager, legislator, node_name, client_fd))));
}

Listener::Listener(Epoll::Manager    &manager,
                   Paxos::Legislator &legislator,
                   const NodeName    &node_name,
                   const char *port)
      : AbstractListener(manager, port),
        legislator(legislator),
        node_name(node_name) {}

}
}
