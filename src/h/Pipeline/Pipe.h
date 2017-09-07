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



#ifndef PIPELINE_PIPE_H
#define PIPELINE_PIPE_H

#include "Pipeline/Segment.h"
#include "Epoll.h"
#include "Paxos/Value.h"
#include "Pipeline/NodeName.h"

#include <fcntl.h>

namespace Pipeline {

template <class Upstream>
class Pipe {
  Pipe           (const Pipe&) = delete; // no copying
  Pipe &operator=(const Pipe&) = delete; // no assignment

private:
  class ReadEnd : public Epoll::Handler {
  private:
    Pipe &pipe;
  public:
    ReadEnd(Pipe &pipe) : pipe(pipe) {}

    void handle_readable() override;
    void handle_writeable() override;
    void handle_error(const uint32_t) override;
  };

  class WriteEnd : public Epoll::Handler {
  private:
    Pipe &pipe;
  public:
    WriteEnd(Pipe &pipe) : pipe(pipe) {}

    void handle_readable() override;
    void handle_writeable() override;
    void handle_error(const uint32_t) override;
  };

        Epoll::Manager            &manager;
        Upstream                  &upstream;

  const NodeName                  &node_name;
  const Paxos::Value::StreamName   stream;

        Segment                   *current_segment = NULL;
        uint64_t                   next_stream_pos;
        uint64_t                   bytes_in_pipe = 0;

        int                        pipe_fds[2];
        ReadEnd                    read_end;
        WriteEnd                   write_end;

  void handle_writeable();
  void close_current_segment();
  void shutdown();
  void unclean_shutdown();

public:
  Pipe (Epoll::Manager&,
        Upstream&,
        const NodeName&,
        const Paxos::Value::StreamName&);

  ~Pipe();

  bool is_shutdown() const;
  void close_write_end();
  void handle_readable();
 
#ifndef NDEBUG
  const uint64_t get_next_stream_pos_write() const {
    return next_stream_pos + bytes_in_pipe;
  }
#endif // ndef NDEBUG

 const int get_write_end_fd() const {
    return pipe_fds[1];
  }

  void wait_until_writeable();

  void record_bytes_in(uint64_t);
};

}

#endif // ndef PIPELINE_PIPE_H
