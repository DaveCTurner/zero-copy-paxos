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



#ifndef PIPELINE_PEER_LISTENER_H
#define PIPELINE_PEER_LISTENER_H

#include "Pipeline/Peer/Socket.h"
#include "Pipeline/AbstractListener.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"

#include <memory>

namespace Pipeline {
namespace Peer {

class Listener : public AbstractListener {

  Paxos::Legislator     &legislator;
  SegmentCache          &segment_cache;
  const NodeName        &node_name;
  std::vector<std::unique_ptr<Socket>> peer_sockets;

  protected:
  void handle_accept(int) override;

  public:
    Listener(Epoll::Manager&,
             SegmentCache&,
             Paxos::Legislator&,
             const NodeName&,
             const char*);
};

}
}

#endif // ndef PIPELINE_PEER_LISTENER_H
