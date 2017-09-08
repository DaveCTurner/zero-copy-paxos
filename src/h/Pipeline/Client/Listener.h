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



#ifndef PIPELINE_CLIENT_LISTENER_H
#define PIPELINE_CLIENT_LISTENER_H

#include "Pipeline/AbstractListener.h"
#include "Pipeline/Client/ChosenStreamContentHandler.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"
#include "Pipeline/Client/Socket.h"

#include <memory>
#include <string.h>

namespace Pipeline {
namespace Client {

class Listener : public AbstractListener,
                 public ChosenStreamContentHandler {

  Paxos::Legislator &legislator;
  const NodeName    &node_name;
  Paxos::Value::StreamId next_stream_id = 0;
  std::vector<std::unique_ptr<Socket>> client_sockets;

  protected:
  void handle_accept(int client_fd) override;

  public:
    Listener(Epoll::Manager&, Paxos::Legislator&,
             const NodeName&, const char*);
    void handle_stream_content(const Paxos::Proposal&);
    void handle_unknown_stream_content(const Paxos::Proposal&);
    void handle_non_contiguous_stream_content(const Paxos::Proposal&);
};

}
}

#endif // ndef PIPELINE_CLIENT_LISTENER_H
