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



#include "Pipeline/Peer/Socket.h"
#include "Pipeline/Pipe.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"

#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

namespace Pipeline {
namespace Peer {

void Socket::ProposalReceiver::shutdown() {
  manager.deregister_close_and_clear(fd);
}

Socket::ProposalReceiver::ProposalReceiver(Epoll::Manager &manager,
          SegmentCache      &segment_cache,
          Paxos::Legislator &legislator,
    const NodeName          &node_name,
          Paxos::NodeId      peer_id,
          int                fd,
    const Paxos::Term       &term,
          Paxos::Value::OffsetStream stream,
          Paxos::Slot        first_slot)
  : manager(manager),
    legislator(legislator),
    node_name(node_name),
    peer_id(peer_id),
    fd(fd),
    proposal({.slots = Paxos::SlotRange(first_slot, first_slot),
              .term = term,
              .value = {.type = Paxos::Value::Type::stream_content}}),
    pipe(manager, *this, segment_cache, node_name,
         stream.name, first_slot - stream.offset) {

  proposal.value.payload.stream = stream;
}

bool Socket::ProposalReceiver::is_shutdown() const { return fd == -1; }

void Socket::ProposalReceiver::handle_readable() {
  assert(fd != -1);
  assert(pipe.get_write_end_fd() != -1);
  assert(!waiting_for_downstream);

  ssize_t splice_result = splice(
    fd, NULL, pipe.get_write_end_fd(), NULL,
    CLIENT_SEGMENT_DEFAULT_SIZE,
    SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE);

  if (splice_result == -1) {
    if (errno == EAGAIN) {
#ifndef NTRACE
      fprintf(stderr, "%s (fd=%d,peer=%d): splice() returned EAGAIN\n",
                      __PRETTY_FUNCTION__, fd, peer_id);
#endif // ndef NTRACE
      pipe.wait_until_writeable();
      manager.modify_handler(fd, this, 0);
      waiting_for_downstream = true;
    } else {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s (fd=%d,peer=%d): splice() failed\n",
                      __PRETTY_FUNCTION__, fd, peer_id);
      shutdown();
    }
  } else if (splice_result == 0) {
    printf("%s (fd=%d,peer=%d): EOF\n",
           __PRETTY_FUNCTION__, fd, peer_id);
    shutdown();
  } else {
    assert(splice_result > 0);
    uint64_t bytes_sent = splice_result;
    pipe.record_bytes_in(bytes_sent);
    pipe.handle_readable();
  }
}

void Socket::ProposalReceiver::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  abort();
}

void Socket::ProposalReceiver::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
}

bool Socket::ProposalReceiver::ok_to_write_data(uint64_t next_stream_pos) {
  Paxos::Slot next_slot = next_stream_pos
                        + proposal.value.payload.stream.offset;
  if   (legislator.proposal_will_be_accepted(proposal)
     && legislator.is_unchosen(next_slot)) {
    return true;
  }

  fprintf(stderr, "%s (fd=%d,peer=%d): not ok, shutting down\n",
                  __PRETTY_FUNCTION__, fd, peer_id);
  shutdown();
  return false;
}

void Socket::ProposalReceiver::downstream_became_writeable() {
  assert(waiting_for_downstream);
  manager.modify_handler(fd, this, EPOLLIN);
  waiting_for_downstream = false;
}

void Socket::ProposalReceiver::downstream_closed() {
  fprintf(stderr, "%s (fd=%d,peer=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd, peer_id);
  shutdown();
}

void Socket::ProposalReceiver::downstream_wrote_bytes
        (uint64_t next_stream_pos,
         uint64_t bytes_sent) {

  proposal.slots.set_end(next_stream_pos + bytes_sent
                          + proposal.value.payload.stream.offset);
  legislator.handle_proposed_and_accepted(peer_id, proposal);
}

const Paxos::Term &Socket::ProposalReceiver::get_term_for_next_write() const {
  return proposal.term;
}

const Paxos::Value::StreamOffset Socket::ProposalReceiver::get_offset_for_next_write(uint64_t) const {
  return proposal.value.payload.stream.offset;
}


}
}
