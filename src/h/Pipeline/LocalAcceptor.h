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



#ifndef PIPELINE_LOCAL_ACCEPTOR_H
#define PIPELINE_LOCAL_ACCEPTOR_H

#include "Epoll.h"
#include "Paxos/Proposal.h"
#include "Pipeline/Pipe.h"
#include "Pipeline/SegmentCache.h"

namespace Pipeline {

class LocalAcceptor {
  class DummyClockCache : public Epoll::ClockCache {
  public:
    void set_current_time(const timestamp&);
  };

  const Paxos::Proposal     &proposal;
        Paxos::SlotRange    &slots_to_accept;
        DummyClockCache      dummy_clock_cache;
        Epoll::Manager       manager;
        Pipe<LocalAcceptor>  pipe;
        std::vector<std::unique_ptr<SegmentCache::CacheEntry>> &entries;

public:
  LocalAcceptor(const Paxos::Proposal  &proposal,
                      Paxos::SlotRange &slots_to_accept,
                      SegmentCache     &segment_cache,
                const NodeName         &node_name,
                      std::vector<std::unique_ptr<SegmentCache::CacheEntry>> &entries);

  bool ok_to_write_data(uint64_t) { return true; }

  const Paxos::Term &get_term_for_next_write() {
    return proposal.term;
  }

  const uint64_t get_offset_for_next_write(uint64_t) {
    return proposal.value.payload.stream.offset;
  }

  void downstream_wrote_bytes(uint64_t, uint64_t);
  void downstream_closed();
  void downstream_became_writeable();

  void run();
};

}

#endif // ndef PIPELINE_LOCAL_ACCEPTOR_H
