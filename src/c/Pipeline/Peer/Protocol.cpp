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



#include "Pipeline/Peer/Protocol.h"
#include <assert.h>
#include <string.h>
#include <unistd.h>

namespace Pipeline {
namespace Peer {
namespace Protocol {

void send_handshake(int fd, const NodeName &node_name) {
  assert(strlen(node_name.cluster.c_str()) == CLUSTER_ID_LENGTH);

  Handshake handshake;
  strncpy(handshake.cluster_id, node_name.cluster.c_str(), CLUSTER_ID_LENGTH);
  handshake.cluster_id[CLUSTER_ID_LENGTH] = '\0';
  handshake.node_id = node_name.id;

  ssize_t handshake_write_result = write(fd, &handshake, sizeof handshake);
  if (handshake_write_result == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s (fd=%d): write(handshake) failed\n",
      __PRETTY_FUNCTION__, fd);
    abort();
  } else if (handshake_write_result != sizeof handshake) {
    fprintf(stderr, "%s (fd=%d): write(handshake) wrote %ld of %ld bytes\n",
      __PRETTY_FUNCTION__, fd, handshake_write_result, sizeof handshake);
    abort();
  }
  // TODO more graceful handling of partial writes?
}

int receive_handshake(int fd, Handshake &handshake, size_t &received_bytes,
                      const std::string &cluster_id) {
  ssize_t read_result = read(fd,
      reinterpret_cast<uint8_t*>(&handshake) + received_bytes,
      sizeof handshake - received_bytes);

  if (read_result == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s (fd=%d): read(handshake) failed\n",
                    __PRETTY_FUNCTION__, fd);
    return RECEIVE_HANDSHAKE_ERROR;
  }

  if (read_result == 0) {
    return RECEIVE_HANDSHAKE_EOF;
  }

  assert(read_result > 0);
  received_bytes += read_result;

  assert(received_bytes <= sizeof handshake);
  if (received_bytes < sizeof handshake) {
    return RECEIVE_HANDSHAKE_INCOMPLETE;
  }

  if (handshake.protocol_version != 1) {
    fprintf(stderr, "%s (fd=%d): protocol version mismatch: %u != %u\n",
      __PRETTY_FUNCTION__, fd,
      handshake.protocol_version, PROTOCOL_VERSION);
    return RECEIVE_HANDSHAKE_INVALID;
  }

  if (handshake.cluster_id[CLUSTER_ID_LENGTH] != '\0') {
    fprintf(stderr, "%s (fd=%d): cluster ID missing null terminator\n",
      __PRETTY_FUNCTION__, fd);
    return RECEIVE_HANDSHAKE_INVALID;
  }

  if (strncmp(cluster_id.c_str(), handshake.cluster_id,
                     CLUSTER_ID_LENGTH) != 0) {

    fprintf(stderr, "%s (fd=%d): cluster ID mismatch: %s != %s\n",
      __PRETTY_FUNCTION__, fd,
      handshake.cluster_id, cluster_id.c_str());
    return RECEIVE_HANDSHAKE_INVALID;
  }

  return RECEIVE_HANDSHAKE_SUCCESS;
}

Paxos::Term Term::get_paxos_term() const {
  return Paxos::Term(era, term_number, owner);
}

void Term::copy_from(const Paxos::Term &src) {
  era         = src.era;
  term_number = src.term_number;
  owner       = src.owner;
}

}}}
