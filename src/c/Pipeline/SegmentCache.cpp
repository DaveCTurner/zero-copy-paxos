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



#include "Pipeline/SegmentCache.h"

#include <algorithm>
#include <unistd.h>
#include <sys/sendfile.h>

namespace Pipeline {

SegmentCache::CacheEntry::CacheEntry
  (const Paxos::Value::OffsetStream &stream,
   const Paxos::Slot                &initial_slot)
      : stream(stream),
        slots(Paxos::SlotRange(initial_slot, initial_slot)) {}

SegmentCache::CacheEntry::~CacheEntry() {
  shutdown();
  assert(fd == -1);
}

void SegmentCache::CacheEntry::shutdown() {
  assert(closed_for_writing);
  if (fd != -1) {
    close(fd);
    fd = -1;
  }
}

void SegmentCache::CacheEntry::extend(uint64_t bytes) {
  slots.set_end(slots.end() + bytes);
}

void SegmentCache::CacheEntry::close_for_writing() {
  closed_for_writing = true;
}

void SegmentCache::CacheEntry::set_fd(const int new_fd) {
  assert(fd == -1);
  assert(!closed_for_writing);
  fd = new_fd;
}

SegmentCache::CacheEntry &SegmentCache::add
  (const Paxos::Value::OffsetStream &stream,
   const Paxos::Slot                 initial_slot) {

  entries.push_back(std::move(std::unique_ptr<CacheEntry>
    (new CacheEntry(stream, initial_slot))));
  return *entries.back();
}

void SegmentCache::expire_because_chosen_to(const Paxos::Slot first_unchosen_slot) {
  entries.erase(std::remove_if(
    entries.begin(),
    entries.end(),
    [first_unchosen_slot](const std::unique_ptr<CacheEntry> &ce) {
      return ce->closed_for_writing
          && ce->slots.end() < first_unchosen_slot;
    }),
    entries.end());
}

SegmentCache::WriteAcceptedDataResult
  SegmentCache::write_accepted_data_to(int out_fd,
              const Paxos::Value::OffsetStream &stream,
              Paxos::SlotRange &slots) {
  if (slots.is_empty()) {
    return SegmentCache::WriteAcceptedDataResult::succeeded;
  }

  const auto it = std::find_if(
    entries.cbegin(),
    entries.cend(),
    [&stream, &slots](const std::unique_ptr<CacheEntry> &ce) {
      return ce->stream.name.owner == stream.name.owner
          && ce->stream.name.id    == stream.name.id
          && ce->stream.offset     == stream.offset
          && ce->slots.contains(slots.start())
          && ce->fd != -1;
    });

  if (it == entries.cend()) {
    fprintf(stderr, "%s: matching CacheEntry not found\n",
                    __PRETTY_FUNCTION__);
    return SegmentCache::WriteAcceptedDataResult::failed;
  }

  CacheEntry &ce = **it;

  assert(slots.start() >= ce.slots.start());
  off_t file_offset = slots.start() - ce.slots.start();

  ssize_t sendfile_result = sendfile(out_fd, ce.fd,
                                     &file_offset,
                                     slots.end() - slots.start());

  if (sendfile_result == -1) {
    if (errno == EAGAIN) {
      return SegmentCache::WriteAcceptedDataResult::blocked;
    }
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: sendfile() failed\n", __PRETTY_FUNCTION__);
    return SegmentCache::WriteAcceptedDataResult::failed;
  }

  assert(sendfile_result > 0);
  slots.truncate(slots.start() + sendfile_result);

  return SegmentCache::WriteAcceptedDataResult::succeeded;
}

}
