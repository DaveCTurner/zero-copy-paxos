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



#ifndef PIPELINE_PEER_SOCKET_H
#define PIPELINE_PEER_SOCKET_H

#include "Pipeline/Peer/Protocol.h"
#include "Pipeline/Pipe.h"
#include "Pipeline/Peer/Protocol.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"

namespace Pipeline {
namespace Peer {

class Socket : public Epoll::Handler {
  Socket           (const Socket&) = delete; // no copying
  Socket &operator=(const Socket&) = delete; // no assignment

private:
        Epoll::Manager            &manager;
        Paxos::Legislator         &legislator;

  const NodeName                  &node_name;

        int                        fd;

  Paxos::NodeId       peer_id = 0;
  Protocol::Handshake received_handshake;
  size_t              received_handshake_size = 0;

  void shutdown();

public:
  Socket(Epoll::Manager&, Paxos::Legislator&,
         const NodeName&, const int);
  ~Socket();

  bool is_shutdown() const;

  void handle_readable() override;

  void handle_writeable() override;

  void handle_error(const uint32_t events) override;
};

}
}

#endif // ndef PIPELINE_PEER_SOCKET_H
