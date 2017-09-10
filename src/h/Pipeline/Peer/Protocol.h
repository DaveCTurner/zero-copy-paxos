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
};

}}}


#endif // ndef PIPELINE_PEER_PROTOCOL_H
