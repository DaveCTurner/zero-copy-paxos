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
#include <fcntl.h>

namespace Pipeline {

LocalAcceptor::LocalAcceptor
  (const Paxos::Proposal  &proposal,
         Paxos::SlotRange &slots_to_accept,
         SegmentCache     &segment_cache,
   const NodeName         &node_name,
         std::vector<std::unique_ptr<SegmentCache::CacheEntry>> &entries)

    : proposal(proposal),
      slots_to_accept(slots_to_accept),
      pipe(manager, *this, segment_cache, node_name, node_name.id,
            proposal.value.payload.stream.name,
            slots_to_accept.start() - proposal.value.payload.stream.offset),
      entries(entries) {
}

void LocalAcceptor::downstream_wrote_bytes(uint64_t, uint64_t) {
}

void LocalAcceptor::downstream_closed() {
}

void LocalAcceptor::downstream_became_writeable() {
  const auto  first_slot_to_accept = slots_to_accept.start();
  const auto &stream               = proposal.value.payload.stream;

  const auto entry_it = std::find_if(
    entries.cbegin(),
    entries.cend(),
    [&stream, &first_slot_to_accept](const std::unique_ptr<SegmentCache::CacheEntry> &ce) {
      return ce->stream.name.owner == stream.name.owner
          && ce->stream.name.id    == stream.name.id
          && ce->stream.offset     == stream.offset
          && ce->slots.contains(first_slot_to_accept)
          && ce->fd                != -1;
    });

  if (entry_it == entries.cend()) {
    std::cout << __PRETTY_FUNCTION__
              << ": no segment for " << stream
              << " containing " << slots_to_accept
              << std::endl;
    pipe.close_write_end();
    return;
  }

  const auto &entry = **entry_it;
  assert(entry.slots.start() <= first_slot_to_accept);
  loff_t off_in = first_slot_to_accept - entry.slots.start();

  ssize_t splice_result = splice(
         entry.fd,                 &off_in,
         pipe.get_write_end_fd(),   NULL,
         slots_to_accept.end() - slots_to_accept.start(),
         SPLICE_F_MOVE | SPLICE_F_NONBLOCK | SPLICE_F_MORE);

  if (splice_result == -1) {
    if (errno == EAGAIN) {
#ifndef NTRACE
      printf("%s: EAGAIN\n", __PRETTY_FUNCTION__);
#endif // ndef NTRACE
      pipe.wait_until_writeable();
    } else {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: splice() failed\n", __PRETTY_FUNCTION__);
      pipe.close_write_end();
    }
  } else if (splice_result == 0) {
    printf("%s: EOF\n", __PRETTY_FUNCTION__);
    pipe.close_write_end();
  } else {
#ifndef NTRACE
    printf("%s: splice_result=%ld\n", __PRETTY_FUNCTION__, splice_result);
#endif // ndef NTRACE
    assert(splice_result > 0);
    uint64_t bytes_sent = splice_result;
    slots_to_accept.truncate(slots_to_accept.start() + bytes_sent);
    pipe.record_bytes_in(bytes_sent);
  }
}

void LocalAcceptor::run() {
  pipe.wait_until_writeable();
  while (!pipe.is_shutdown()) {
    manager.wait(1000);
  }
}

}
