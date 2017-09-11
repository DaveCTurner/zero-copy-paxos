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



#ifndef PIPELINE_PEER_PROTOCOL_H
#define PIPELINE_PEER_PROTOCOL_H

#include "Paxos/Term.h"
#include "Paxos/Value.h"
#include "Pipeline/NodeName.h"

#define CLUSTER_ID_LENGTH 36  // length of a GUID string
#define PROTOCOL_VERSION  1

namespace Pipeline {
namespace Peer {
namespace Protocol {

struct Handshake {
  uint32_t      protocol_version = PROTOCOL_VERSION;
  char          cluster_id[CLUSTER_ID_LENGTH+1];
  Paxos::NodeId node_id;
} __attribute__((packed));

void send_handshake(int, const NodeName&);

#define RECEIVE_HANDSHAKE_ERROR       0
#define RECEIVE_HANDSHAKE_INCOMPLETE  1
#define RECEIVE_HANDSHAKE_EOF         2
#define RECEIVE_HANDSHAKE_INVALID     3
#define RECEIVE_HANDSHAKE_SUCCESS     4

int receive_handshake(int, Handshake&, size_t&, const std::string&);

/*

Protocol - start with a handshake:
- 4 bytes protocol version
- 36 bytes cluster ID
- 1 byte null terminator
- 4 bytes node ID

Then sequence of messages. Each message starts
with an identifying byte followed by some (or fewer)
bytes according to its type.

*/

struct Term {
  Paxos::Era        era;
  Paxos::TermNumber term_number;
  Paxos::NodeId     owner;

  Paxos::Term get_paxos_term() const;
  void copy_from(const Paxos::Term&);
} __attribute__((packed));

union Message {

/* Type 0x01: seek_votes_or_catch_up(const Slot&, const Term&)
    - 8 bytes slot number
    - 12 bytes term (4 bytes era, 4 bytes term number, 4 bytes owner id)
*/

#define MESSAGE_TYPE_SEEK_VOTES_OR_CATCH_UP 0x01
  struct seek_votes_or_catch_up {
    Paxos::Slot slot;
    Term        term;
  } __attribute__((packed));
  seek_votes_or_catch_up      seek_votes_or_catch_up;

/* Type 0x02: offer_vote(const NodeId&, const Term&)
    - (NodeId parameter is destination, not included in message)
    - 12 bytes term (4 bytes era, 4 bytes term number, 4 bytes owner id)
*/

#define MESSAGE_TYPE_OFFER_VOTE 0x02
  struct offer_vote {
    Term        term;
  } __attribute__((packed));
  offer_vote                  offer_vote;

/* Type 0x03: offer_catch_up(const NodeId&)
    - (NodeId parameter is destination, not included in message)
    - no further data
*/

#define MESSAGE_TYPE_OFFER_CATCH_UP 0x03

/* Type 0x04: request_catch_up(const NodeId&)
    - (NodeId parameter is destination, not included in message)
    - no further data
*/

#define MESSAGE_TYPE_REQUEST_CATCH_UP 0x04

/* Type 0x05: send_catch_up(const NodeId&, const Slot&, const Era&,
                            const Configuration&, const NodeId&, const NodeId&,
                            const Value::StreamId&, const * uint64_t)
    - (first NodeId parameter is destination, not included in message)
    - 8 bytes slot number
    - 4 bytes era
    - 4 bytes next-generated node id
    - 4 bytes current stream owner
    - 4 bytes current stream id
    - 8 bytes current stream position
    - Configuration:
      - 4 bytes entry count, then repeated this many times:
        - 4 bytes node id
        - 1 byte weight
*/

#define MESSAGE_TYPE_SEND_CATCH_UP 0x05
  struct send_catch_up {
    Paxos::Slot            slot;
    Paxos::Era             era;
    Paxos::NodeId          next_generated_node_id;
    Paxos::NodeId          current_stream_owner;
    Paxos::Value::StreamId current_stream_id;
    uint64_t               current_stream_position;
    uint32_t               configuration_size;
  } __attribute__((packed));
  send_catch_up               send_catch_up;

  struct configuration_entry {
    Paxos::NodeId                node_id;
    Paxos::Configuration::Weight weight;
  } __attribute__((packed));

/* Type 0x06: prepare_term(const Term&)
    - 12 bytes term (4 bytes era, 4 bytes term number, 4 bytes owner id)
*/

#define MESSAGE_TYPE_PREPARE_TERM 0x06
  struct prepare_term {
    Term        term;
  } __attribute__((packed));
  prepare_term                prepare_term;

/* Type 0x07: make_promise(const Promise& == Promise::Type::multi)
    - 8 bytes slot number
    - 12 bytes term (4 bytes era, 4 bytes term number, 4 bytes owner id)
*/

#define MESSAGE_TYPE_MAKE_PROMISE_MULTI 0x07
  struct make_promise_multi {
    Paxos::Slot slot;
    Term        term;
  } __attribute__((packed));
  make_promise_multi          make_promise_multi;

/* Type 0x08: make_promise(const Promise& == Promise::Type::free)
    - 16 bytes slot range (8 byte slot number *2)
    - 12 bytes term (4 bytes era, 4 bytes term number, 4 bytes owner id)
*/

#define MESSAGE_TYPE_MAKE_PROMISE_FREE 0x08
  struct make_promise_free {
    Paxos::Slot start_slot;
    Paxos::Slot end_slot;
    Term        term;
  } __attribute__((packed));
  make_promise_free           make_promise_free;

};

}}}


#endif // ndef PIPELINE_PEER_PROTOCOL_H
