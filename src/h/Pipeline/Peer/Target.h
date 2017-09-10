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



#ifndef PIPELINE_PEER_TARGET_H
#define PIPELINE_PEER_TARGET_H

#include "Epoll.h"
#include "Paxos/Legislator.h"
#include "Pipeline/NodeName.h"

namespace Pipeline {
namespace Peer {

class Target : public Epoll::Handler {
  Target           (const Target&) = delete; // no copying
  Target &operator=(const Target&) = delete; // no assignment

public:
  class Address {
  public:
    const std::string host;
    const std::string port;

    Address(const char *host, const char *port);
  };

private:
  const Address             address;
        Epoll::Manager     &manager;
        Paxos::Legislator  &legislator;
  const NodeName           &node_name;

        int                 fd = -1;
        bool                sent_handshake = false;

  void shutdown();

public:
  Target(const Address           &address,
               Epoll::Manager    &manager,
               Paxos::Legislator &legislator,
         const NodeName          &node_name);

  void handle_readable() override;
  void handle_writeable() override;
  void handle_error(const uint32_t) override;

  void start_connection();

};

}}

#endif // ndef PIPELINE_PEER_TARGET_H
