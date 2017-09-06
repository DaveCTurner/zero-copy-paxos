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
#include "Epoll.h"
#include "Paxos/Legislator.h"

#include <memory>
#include <string.h>

namespace Pipeline {
namespace Client {

class Listener : public AbstractListener {

  Paxos::Legislator &legislator;
  const std::string &cluster_name;
  const Paxos::NodeId node_id;
  Paxos::Value::StreamId next_stream_id = 0;

  protected:
  void handle_accept(int client_fd) override;

  public:
    Listener(Epoll::Manager&, Paxos::Legislator&,
             const std::string&, const Paxos::NodeId, const char*);
};

}
}

#endif // ndef PIPELINE_CLIENT_LISTENER_H
