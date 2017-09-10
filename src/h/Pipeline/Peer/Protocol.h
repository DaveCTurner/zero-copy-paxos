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

}}}


#endif // ndef PIPELINE_PEER_PROTOCOL_H
