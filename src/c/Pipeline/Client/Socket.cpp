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
}

Socket::~Socket() {
  shutdown();
}

bool Socket::is_shutdown() const {
  return fd == -1;
}

void Socket::handle_readable() {
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
    assert(splice_result > 0);
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
  Paxos::Value value = { .type = Paxos::Value::Type::stream_content };

  uint64_t next_slot = legislator.get_next_activated_slot();
  assert(start_pos <= next_slot);

  value.payload.stream = {
    .name = stream,
    .offset = next_slot - start_pos
  };

  legislator.activate_slots(value, byte_count);
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
