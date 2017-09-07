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
  fprintf(stderr, "%s: TODO\n", __PRETTY_FUNCTION__);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::handle_writeable() {
  fprintf(stderr, "%s: TODO\n", __PRETTY_FUNCTION__);
  abort();
}

template<class Upstream>
void Pipe<Upstream>::shutdown () {
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

  manager.register_handler(pipe_fds[0], &read_end,  EPOLLIN);
  manager.register_handler(pipe_fds[1], &write_end, 0);
}

template<class Upstream>
Pipe<Upstream>::~Pipe() {
  shutdown();
}

template<class Upstream>
bool Pipe<Upstream>::is_shutdown() const {
  return pipe_fds[0] == -1;
}

template<class Upstream>
void Pipe<Upstream>::close_write_end() {
  manager.deregister_close_and_clear(pipe_fds[1]);
}

template class Pipe<Client::Socket>;

}
