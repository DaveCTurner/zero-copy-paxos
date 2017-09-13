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



#include "Pipeline/LocalAcceptor.h"

#include <algorithm>
#include <unistd.h>
#include <sys/sendfile.h>

namespace Pipeline {

SegmentCache::CacheEntry::CacheEntry
  (const Paxos::Value::OffsetStream &stream,
   const Paxos::Slot                &initial_slot,
   const bool                        is_locally_accepted)
      : stream(stream),
        slots(Paxos::SlotRange(initial_slot, initial_slot)),
        is_locally_accepted(is_locally_accepted) {}

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
   const Paxos::Slot                 initial_slot,
         bool                        is_locally_accepted) {

  entries.push_back(std::move(std::unique_ptr<CacheEntry>
    (new CacheEntry(stream, initial_slot, is_locally_accepted))));
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
          && ce->is_locally_accepted
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

#ifndef NDEBUG
  off_t current_offset = lseek(ce.fd, 0, SEEK_CUR);
  assert(0 <= current_offset);
#endif // ndef NDEBUG

  ssize_t sendfile_result = sendfile(out_fd, ce.fd,
                                     &file_offset,
                                     slots.end() - slots.start());

  assert(current_offset == lseek(ce.fd, 0, SEEK_CUR));

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

void SegmentCache::locally_accept(const Paxos::Proposal &proposal,
                                        Paxos::SlotRange &slots_to_accept) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ": proposal="       << proposal
                                   << " slots_to_ensure=" << slots_to_accept
                                   << std::endl;

  for (const auto &pce : entries) {
    std::cout << __PRETTY_FUNCTION__ << ": cache entry stream=" << pce->stream
                                     << " slots="               << pce->slots
                                     << std::endl;
  }
#endif // def NTRACE

  LocalAcceptor local_acceptor(proposal, slots_to_accept,
                               *this, node_name, entries);
  local_acceptor.run();
}

void SegmentCache::ensure_locally_accepted(const Paxos::Proposal &proposal) {
  assert(proposal.value.type == Paxos::Value::Type::stream_content);

  Paxos::SlotRange slots_to_ensure = proposal.slots;
  const auto &stream               = proposal.value.payload.stream;

  while (slots_to_ensure.is_nonempty()) {
    const Paxos::Slot first_slot_to_ensure = slots_to_ensure.start();

    const auto locally_accepted_it = std::find_if(
      entries.cbegin(),
      entries.cend(),
      [&stream, &first_slot_to_ensure](const std::unique_ptr<CacheEntry> &ce) {
        return ce->stream.name.owner == stream.name.owner
            && ce->stream.name.id    == stream.name.id
            && ce->stream.offset     == stream.offset
            && ce->slots.contains(first_slot_to_ensure)
            && ce->is_locally_accepted;
      });

    if (locally_accepted_it == entries.cend()) {
      // Here, have a nonempty range of slots to fill, and the first slot is
      // not locally accepted. Therefore we must have an acceptance from a
      // bound promise that needs to be copied across to a local acceptance
      // file.

      locally_accept(proposal, slots_to_ensure);
      return;
    }

    const CacheEntry &locally_accepted = **locally_accepted_it;
    slots_to_ensure.truncate(locally_accepted.slots.end());
  }
}

}
