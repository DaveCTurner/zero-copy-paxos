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
#include "Pipeline/Peer/Protocol.h"

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
  struct CurrentMessage {
    uint8_t           type = 0xff;
    Protocol::Message message;
    size_t            still_to_send = 0;
  }                          current_message;
  bool                       waiting_to_become_writeable = true;
  bool prepare_to_send(uint8_t);

  const Address             address;
        Epoll::Manager     &manager;
        Paxos::Legislator  &legislator;
  const NodeName           &node_name;
        Paxos::NodeId       peer_id = 0;

        int                 fd = -1;

        bool                sent_handshake = false;
        Protocol::Handshake received_handshake;
        size_t              received_handshake_bytes = 0;

  bool is_connected() const;
  bool is_connected_to(const Paxos::NodeId &n) const;
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

  void seek_votes_or_catch_up(const Paxos::Slot &first_unchosen_slot,
                              const Paxos::Term &min_acceptable_term);
  void offer_vote(const Paxos::NodeId &destination,
                  const Paxos::Term   &min_acceptable_term);
  void offer_catch_up(const Paxos::NodeId &destination);
  void request_catch_up(const Paxos::NodeId &destination);

};

}}

#endif // ndef PIPELINE_PEER_TARGET_H
