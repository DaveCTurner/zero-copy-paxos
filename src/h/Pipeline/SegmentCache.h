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



#ifndef PIPELINE_SEGMENT_CACHE_H
#define PIPELINE_SEGMENT_CACHE_H

#include "Paxos/Proposal.h"
#include "Pipeline/NodeName.h"
#include <memory>

namespace Pipeline {

class SegmentCache {
public:
  SegmentCache           (const SegmentCache&) = delete;
  SegmentCache &operator=(const SegmentCache&) = delete;

  struct CacheEntry {
    const Paxos::Value::OffsetStream stream;
          Paxos::SlotRange           slots;
          bool                       closed_for_writing = false;
          int                        fd = -1;

    CacheEntry(const Paxos::Value::OffsetStream &stream,
               const Paxos::Slot                &initial_slot);

    ~CacheEntry();

    void shutdown();
    void extend(uint64_t bytes);
    void close_for_writing();
    void set_fd(const int new_fd);
    CacheEntry           (const CacheEntry&) = delete;
    CacheEntry &operator=(const CacheEntry&) = delete;
  };

private:
  std::vector<std::unique_ptr<CacheEntry>> entries;
  const NodeName &node_name;

public:
  SegmentCache(const NodeName &node_name)
    : node_name(node_name) {}

  CacheEntry &add(const Paxos::Value::OffsetStream &stream,
                  const Paxos::Slot                 initial_slot);

  void expire_because_chosen_to(const Paxos::Slot first_unchosen_slot);

  enum WriteAcceptedDataResult : uint8_t {
    failed,
    blocked,
    succeeded
  };

  WriteAcceptedDataResult
    write_accepted_data_to(int out_fd,
                const Paxos::Value::OffsetStream &stream,
                Paxos::SlotRange &slots);
};


}


#endif // ndef PIPELINE_SEGMENT_CACHE_H
