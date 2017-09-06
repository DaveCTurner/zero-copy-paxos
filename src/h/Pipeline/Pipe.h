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

#include "Epoll.h"
#include "Paxos/Value.h"
#include "Pipeline/NodeName.h"

#include <fcntl.h>

namespace Pipeline {

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

  const NodeName                  &node_name;
  const Paxos::Value::StreamName   stream;

        uint64_t                   next_stream_pos;
        int                        pipe_fds[2];
        ReadEnd                    read_end;
        WriteEnd                   write_end;

  void handle_writeable();
  void shutdown();

public:
  Pipe (Epoll::Manager&,
        const NodeName&,
        const Paxos::Value::StreamName&);

  ~Pipe();

  bool is_shutdown() const;
  void close_write_end();
  void handle_readable();

};

}

#endif // ndef PIPELINE_PIPE_H
