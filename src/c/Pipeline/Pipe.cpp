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

void Pipe::ReadEnd::handle_readable() {
  pipe.handle_readable();
}

void Pipe::WriteEnd::handle_writeable() {
  pipe.handle_writeable();
}

void Pipe::ReadEnd::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[0]);
  abort();
}

void Pipe::WriteEnd::handle_readable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[1]);
  abort();
}

void Pipe::ReadEnd::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[0], events);
  abort();
}

void Pipe::WriteEnd::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, pipe.pipe_fds[1], events);
  abort();
}

void Pipe::handle_readable() {
  fprintf(stderr, "%s: TODO\n", __PRETTY_FUNCTION__);
  abort();
}

void Pipe::handle_writeable() {
  fprintf(stderr, "%s: TODO\n", __PRETTY_FUNCTION__);
  abort();
}

void Pipe::shutdown () {
  manager.deregister_close_and_clear(pipe_fds[1]);
  manager.deregister_close_and_clear(pipe_fds[0]);
}

Pipe::Pipe
       (Epoll::Manager                  &manager,
        const NodeName                  &node_name,
        const Paxos::Value::StreamName  &stream)
  : manager         (manager),
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

  manager.register_handler(pipe_fds[0], &read_end,  EPOLLIN);
  manager.register_handler(pipe_fds[1], &write_end, 0);
}

Pipe::~Pipe() {
  shutdown();
}

bool Pipe::is_shutdown() const {
  return pipe_fds[0] == -1;
}

void Pipe::close_write_end() {
  manager.deregister_close_and_clear(pipe_fds[1]);
}

}
