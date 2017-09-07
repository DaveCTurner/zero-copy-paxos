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



#include "Pipeline/Client/Socket.h"

namespace Pipeline {
namespace Client {

void Socket::shutdown() {
  send_pending_acknowledgement(false);
  pipe.close_write_end();
  manager.deregister_close_and_clear(fd);
}

Socket::Socket
       (Epoll::Manager                  &manager,
        Paxos::Legislator               &legislator,
        const NodeName                  &node_name,
        const Paxos::Value::StreamName   stream,
        const int                        fd)
  : manager         (manager),
    legislator      (legislator),
    node_name       (node_name),
    stream          (stream),
    pipe            (manager, *this, node_name, stream),
    fd              (fd) {

  manager.register_handler(fd, this, EPOLLIN);

#ifndef NTRACE
  printf("%s: fd=%d\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
}

Socket::~Socket() {
#ifndef NTRACE
  printf("%s: fd=%d\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

  shutdown();
}

bool Socket::is_shutdown() const {
  return fd == -1;
}

void Socket::handle_readable() {
  assert(pipe.get_next_stream_pos_write() == read_stream_pos);

  ssize_t splice_result = splice(
    fd, NULL, pipe.get_write_end_fd(), NULL,
    (1<<20), SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE);

  if (splice_result == -1) {
    if (errno == EAGAIN) {
#ifndef NTRACE
      printf("%s: EAGAIN (fd=%d)\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
      pipe.wait_until_writeable();
      manager.modify_handler(fd, this, 0);
    } else {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: splice() failed\n", __PRETTY_FUNCTION__);
      abort();
    }
  } else if (splice_result == 0) {
    printf("%s: EOF\n", __PRETTY_FUNCTION__);
    shutdown();
  } else {
#ifndef NTRACE
    printf("%s: splice_result=%ld (fd=%d)\n", __PRETTY_FUNCTION__, splice_result, fd);
#endif // ndef NTRACE
    assert(splice_result > 0);
    uint64_t bytes_sent = splice_result;
#ifndef NDEBUG
    read_stream_pos += bytes_sent;
#endif // ndef NDEBUG
    pipe.record_bytes_in(bytes_sent);
  }
}

void Socket::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  abort();
}

void Socket::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
}

void Socket::downstream_became_writeable() {
  manager.modify_handler(fd, this, EPOLLIN);
}

bool Socket::ok_to_write_data() const {
  return legislator.activation_will_yield_proposals();
}

void Socket::downstream_wrote_bytes(uint64_t start_pos, uint64_t byte_count) {
  assert(written_stream_pos == start_pos);
#ifndef NDEBUG
  written_stream_pos += byte_count;
#endif // ndef NDEBUG

#ifndef NTRACE
  printf("%s: written_stream_pos updated by %lu from %lu to %lu\n",
          __PRETTY_FUNCTION__, byte_count, start_pos, written_stream_pos);
#endif // def NTRACE
  assert(legislator.activation_will_yield_proposals());

  Paxos::Value value = { .type = Paxos::Value::Type::stream_content };

  uint64_t next_slot = legislator.get_next_activated_slot();
  assert(start_pos <= next_slot);

  value.payload.stream = {
    .name = stream,
    .offset = next_slot - start_pos
  };

  legislator.activate_slots(value, byte_count);
  send_pending_acknowledgement(true);
}

void Socket::send_pending_acknowledgement(bool shutdown_on_error) {
  if (fd == -1) {
    return;
  }

  assert(committed_stream_pos <= written_stream_pos);
  assert(acknowledged_stream_pos <= committed_stream_pos);

  const uint32_t max_acknowledgement = 0xffffffff;

  while (acknowledged_stream_pos < committed_stream_pos) {
    uint64_t acknowledgement_size = committed_stream_pos
                                  - acknowledged_stream_pos;
    uint32_t wire_value = acknowledgement_size <= max_acknowledgement
                        ? acknowledgement_size :  max_acknowledgement;

    ssize_t write_result = write(fd, &wire_value, sizeof wire_value);
    if (write_result == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s (fd=%d): write failed\n", __PRETTY_FUNCTION__, fd);
      if (shutdown_on_error) {
        shutdown();
      }
      return;
    } else {
      if (write_result != sizeof wire_value) {
        fprintf(stderr, "%s: write only sent %ld bytes of %lu\n",
            __PRETTY_FUNCTION__,
            write_result,
            sizeof wire_value);
        shutdown();
        return;
      } else {
        acknowledged_stream_pos += acknowledgement_size;
      }
    }
  }
}

void Socket::handle_stream_content(const Paxos::Proposal &proposal) {
  if (proposal.value.payload.stream.name.owner != stream.owner) {
    shutdown();
    return;
  }

  if (proposal.value.payload.stream.name.id != stream.id) {
    shutdown();
    return;
  }

  committed_stream_pos = proposal.slots.end()
                       - proposal.value.payload.stream.offset;

  send_pending_acknowledgement(true);
}

void Socket::handle_unknown_stream_content(const Paxos::Proposal &proposal) {
  shutdown_if_self(proposal);
}

void Socket::handle_non_contiguous_stream_content(const Paxos::Proposal &proposal) {
  shutdown_if_self(proposal);
}

void Socket::shutdown_if_self(const Paxos::Proposal &proposal) {
  if  (proposal.value.payload.stream.name.owner == stream.owner
    && proposal.value.payload.stream.name.id    == stream.id) {
    shutdown();
  }
}

const Paxos::Term &Socket::get_term_for_next_write() const {
  return legislator.get_next_activated_term();
}

const Paxos::Value::StreamOffset Socket::get_offset_for_next_write
                                      (uint64_t next_stream_pos) const {
  Paxos::Slot next_activated_slot = legislator.get_next_activated_slot();
  assert(next_stream_pos <= next_activated_slot);
  return next_activated_slot - next_stream_pos;
}

}
}
