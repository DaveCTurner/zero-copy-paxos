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

  class PromiseReceiver : public Epoll::Handler {
          Epoll::Manager            &manager;
          Paxos::Legislator         &legislator;
    const NodeName                  &node_name;
    const Paxos::NodeId              peer_id;
          int                        fd;
          Paxos::Promise             promise;
          Pipe<PromiseReceiver>      pipe;
          bool                       waiting_for_downstream = false;

    void shutdown();

  public:
    PromiseReceiver(Epoll::Manager &manager,
              SegmentCache      &segment_cache,
              Paxos::Legislator &legislator,
        const NodeName          &node_name,
              Paxos::NodeId      peer_id,
              int                fd,
        const Paxos::Term       &term,
        const Paxos::Term       &max_accepted_term,
              Paxos::Value::OffsetStream,
              Paxos::Slot        first_slot);

    bool is_shutdown() const;
    void handle_readable() override;
    void handle_writeable() override;
    void handle_error(const uint32_t) override;

    bool ok_to_write_data(uint64_t);
    const Paxos::Term &get_term_for_next_write() const;
    const Paxos::Value::StreamOffset get_offset_for_next_write(uint64_t) const;

    void downstream_became_writeable();
    void downstream_closed();
    void downstream_wrote_bytes(uint64_t next_stream_pos, uint64_t bytes_sent);
  };


  class ProposalReceiver : public Epoll::Handler {
          Epoll::Manager            &manager;
          Paxos::Legislator         &legislator;
    const NodeName                  &node_name;
    const Paxos::NodeId              peer_id;
          int                        fd;
          Paxos::Proposal            proposal;
          Pipe<ProposalReceiver>     pipe;
          bool                       waiting_for_downstream = false;

    void shutdown();

  public:
    ProposalReceiver(Epoll::Manager &manager,
              SegmentCache      &segment_cache,
              Paxos::Legislator &legislator,
        const NodeName          &node_name,
              Paxos::NodeId      peer_id,
              int                fd,
        const Paxos::Term       &term,
              Paxos::Value::OffsetStream,
              Paxos::Slot        first_slot);

    bool is_shutdown() const;
    void handle_readable() override;
    void handle_writeable() override;
    void handle_error(const uint32_t) override;

    bool ok_to_write_data(uint64_t);
    const Paxos::Term &get_term_for_next_write() const;
    const Paxos::Value::StreamOffset get_offset_for_next_write(uint64_t) const;

    void downstream_became_writeable();
    void downstream_closed();
    void downstream_wrote_bytes(uint64_t next_stream_pos, uint64_t bytes_sent);
  };

        Epoll::Manager            &manager;
        SegmentCache              &segment_cache;
        Paxos::Legislator         &legislator;

  const NodeName                  &node_name;

        std::unique_ptr<PromiseReceiver>  promise_receiver  = NULL;
        std::unique_ptr<ProposalReceiver> proposal_receiver = NULL;

        int                        fd;

  Paxos::NodeId       peer_id = 0;
  Protocol::Handshake received_handshake;
  size_t              received_handshake_size = 0;

  uint8_t           current_message_type = 0;
  Protocol::Message current_message;
  Protocol::Value   current_value;
  size_t            size_received = 0;
  bool get_paxos_value(Paxos::Value&);

  std::vector<Paxos::Configuration::Entry> received_entries;
  Protocol::Message::configuration_entry   current_entry;
  size_t                                   current_entry_size = 0;

  void shutdown();

public:
  Socket(Epoll::Manager&, SegmentCache&, Paxos::Legislator&,
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
