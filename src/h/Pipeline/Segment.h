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



#ifndef PIPELINE_SEGMENT_H
#define PIPELINE_SEGMENT_H

#include "Paxos/Value.h"
#include "Paxos/Term.h"
#include "Pipeline/NodeName.h"

namespace Pipeline {

class Segment {
  Segment           (const Segment&) = delete; // no copying
  Segment &operator=(const Segment&) = delete; // no assignment

private:
  uint64_t next_stream_pos;
  uint64_t remaining_space;
  int      fd = -1;
  const Paxos::Term                &term;
  const Paxos::Value::StreamOffset  stream_offset;

public:

#define CLIENT_SEGMENT_DEFAULT_SIZE_BITS 24 // 16MB
#define CLIENT_SEGMENT_DEFAULT_SIZE (1ul<<CLIENT_SEGMENT_DEFAULT_SIZE_BITS)

  Segment(const NodeName&,
          const Paxos::Value::OffsetStream,
          const Paxos::Term&,
          const uint64_t);

  ~Segment();

  void shutdown();

  bool is_shutdown() const { return fd == -1; }

  int get_fd() const {
    assert(!is_shutdown());
    return fd;
  }

  ssize_t get_remaining_space() const {
    assert(!is_shutdown());
    return remaining_space;
  }

  uint64_t get_next_stream_pos() const {
    assert(!is_shutdown());
    return next_stream_pos;
  }

  void record_bytes_in(uint64_t);

  const Paxos::Term &get_term() const {
    return term;
  }

  const Paxos::Value::StreamOffset &get_stream_offset() const {
    return stream_offset;
  }
};

}

#endif // ndef PIPELINE_SEGMENT_H
