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



#include "Pipeline/Pipe.h"
#include "Pipeline/Client/Socket.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

namespace Pipeline {

template<class Upstream>
void Pipe<Upstream>::ReadEnd::handle_readable() {
  pipe.handle_readable();
}

template<class Upstream>
void Pipe<Upstream>::WriteEnd::handle_writeable() {
  pipe.handle_writeable();
}

template<class Upstream>
void Pipe<Upstream>::ReadEnd::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[0]);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::WriteEnd::handle_readable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[1]);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::ReadEnd::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[0], events);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::WriteEnd::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[1], events);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::handle_readable() {
  if (!upstream.ok_to_write_data()) {
    fprintf(stderr, "%s: cancelled by upstream\n", __PRETTY_FUNCTION__);
    close_current_segment();
    manager.deregister_close_and_clear(pipe_fds[1]);
    manager.deregister_close_and_clear(pipe_fds[0]);
    upstream.downstream_closed();
    return;
  }

  const Paxos::Term &term_for_next_write
    = upstream.get_term_for_next_write();
  const Paxos::Value::StreamOffset offset_for_next_write
    = upstream.get_offset_for_next_write(next_stream_pos);

  if (current_segment != NULL
        && (current_segment->get_term()          != term_for_next_write
         || current_segment->get_stream_offset() != offset_for_next_write)) {
    delete current_segment;
    current_segment = NULL;
  }

  if (current_segment == NULL) {
    Paxos::Value::OffsetStream os = {.name = stream, .offset = offset_for_next_write};
    current_segment = new Segment(node_name, os,
        term_for_next_write, next_stream_pos);
  }

  ssize_t splice_result = splice(
    pipe_fds[0], NULL, current_segment->get_fd(), NULL,
    current_segment->get_remaining_space(),
    SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE);

  if (splice_result == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: splice() failed\n", __PRETTY_FUNCTION__);
    abort();
  } else if (splice_result == 0) {
    printf("%s: EOF\n", __PRETTY_FUNCTION__);
    shutdown();
    upstream.downstream_closed();
  } else {
    assert(splice_result > 0);
    uint64_t bytes_sent = splice_result;
    assert(bytes_sent <= bytes_in_pipe);
    bytes_in_pipe -= bytes_sent;
    uint64_t old_next_stream_pos = next_stream_pos;
    next_stream_pos += bytes_sent;
    current_segment->record_bytes_in(bytes_sent);

    if (current_segment->is_shutdown()) {
      close_current_segment();
    }

    upstream.downstream_wrote_bytes(old_next_stream_pos, bytes_sent);
  }
}

template<class Upstream>
void Pipe<Upstream>::handle_writeable() {
  fprintf(stderr, "%s: TODO\n", __PRETTY_FUNCTION__);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::close_current_segment() {
  if (current_segment != NULL) {
    delete current_segment;
    current_segment = NULL;
  }
}

template<class Upstream>
void Pipe<Upstream>::shutdown () {
#ifndef NTRACE
  printf("%s: fds=[%d,%d]\n", __PRETTY_FUNCTION__, pipe_fds[0], pipe_fds[1]);
#endif // ndef NTRACE
  close_current_segment();
  assert(bytes_in_pipe == 0);
  manager.deregister_close_and_clear(pipe_fds[1]);
  manager.deregister_close_and_clear(pipe_fds[0]);
}

template<class Upstream>
Pipe<Upstream>::Pipe
       (Epoll::Manager                  &manager,
        Upstream                        &upstream,
        const NodeName                  &node_name,
        const Paxos::Value::StreamName  &stream)
  : manager         (manager),
    upstream        (upstream),
    node_name       (node_name),
    stream          (stream),
    next_stream_pos (0),
    read_end        (ReadEnd(*this)),
    write_end       (WriteEnd(*this)) {

  if (pipe2(pipe_fds, O_NONBLOCK) == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: pipe2() failed\n", __PRETTY_FUNCTION__);
    abort();
  }

#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ": "
            << stream << " "
            << "fds=[" << pipe_fds[0] << "," << pipe_fds[1] << "]"
            << std::endl;
#endif // ndef NTRACE

  manager.register_handler(pipe_fds[0], &read_end,  EPOLLIN);
  manager.register_handler(pipe_fds[1], &write_end, 0);
}

template<class Upstream>
Pipe<Upstream>::~Pipe() {
#ifndef NTRACE
  printf("%s: fds=[%d,%d]\n", __PRETTY_FUNCTION__, pipe_fds[0], pipe_fds[1]);
#endif // ndef NTRACE
  shutdown();
}

template<class Upstream>
bool Pipe<Upstream>::is_shutdown() const {
  return pipe_fds[0] == -1;
}

template<class Upstream>
void Pipe<Upstream>::close_write_end() {
#ifndef NTRACE
  printf("%s: write end fd=%d\n", __PRETTY_FUNCTION__, pipe_fds[1]);
#endif // ndef NTRACE
  manager.deregister_close_and_clear(pipe_fds[1]);
}

template<class Upstream>
void Pipe<Upstream>::wait_until_writeable() {
  assert(!is_shutdown());
  assert(pipe_fds[1] != -1);
  manager.modify_handler(pipe_fds[1], &write_end, EPOLLOUT);
}

template<class Upstream>
void Pipe<Upstream>::record_bytes_in(uint64_t bytes) {
  assert(!is_shutdown());
  bytes_in_pipe += bytes;
}

template class Pipe<Client::Socket>;

}
