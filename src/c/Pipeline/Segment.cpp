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



#include "Pipeline/Segment.h"
#include "directories.h"

#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

namespace Pipeline {

Segment::Segment
       (      SegmentCache               &segment_cache,
        const NodeName                   &node_name,
        const Paxos::NodeId               acceptor_id,
        const Paxos::Value::OffsetStream  stream,
        const Paxos::Term                &term,
        const uint64_t                    first_stream_pos)
  : next_stream_pos (first_stream_pos)
  , remaining_space(CLIENT_SEGMENT_DEFAULT_SIZE
      - (first_stream_pos & (CLIENT_SEGMENT_DEFAULT_SIZE-1)))
  , term(term)
  , stream_offset(stream.offset)
  , cache_entry(segment_cache.add(stream,
                                  first_stream_pos + stream.offset,
                                  node_name.id == acceptor_id)) {

  char path[PATH_MAX], parent[PATH_MAX];

  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x",
          node_name.cluster.c_str(), node_name.id));

  strncpy(parent, path, PATH_MAX);
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x/own_%08x_str_%08x",
          node_name.cluster.c_str(), node_name.id,
          stream.name.owner,
          stream.name.id));
  ensure_directory(parent, path);

  strncpy(parent, path, PATH_MAX);
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x/own_%08x_str_%08x/off_%016lx",
          node_name.cluster.c_str(), node_name.id,
          stream.name.owner,
          stream.name.id,
          stream.offset));
  ensure_directory(parent, path);

  strncpy(parent, path, PATH_MAX);
  if (acceptor_id == node_name.id) {
    ensure_length(snprintf(path, PATH_MAX,
            "data/clu_%s/n_%08x/own_%08x_str_%08x/off_%016lx/pos_%016lx_trm_%08x_%08x_%08x",
            node_name.cluster.c_str(), node_name.id,
            stream.name.owner,
            stream.name.id,
            stream.offset,
            next_stream_pos,
            term.era, term.term_number, term.owner));
  } else {
    ensure_length(snprintf(path, PATH_MAX,
            "data/clu_%s/n_%08x/own_%08x_str_%08x/off_%016lx/pos_%016lx_trm_%08x_%08x_%08x_by_%08x",
            node_name.cluster.c_str(), node_name.id,
            stream.name.owner,
            stream.name.id,
            stream.offset,
            next_stream_pos,
            term.era, term.term_number, term.owner,
            acceptor_id));
  }
  fd = open(path, O_CREAT | O_RDWR, 0644);
  if (fd == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: open(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }

  cache_entry.set_fd(fd);

  sync_directory(parent);

#ifndef NTRACE
  printf("%s: opened segment file %s with fd %d\n",
    __PRETTY_FUNCTION__, path, fd);
#endif // ndef NTRACE
}

Segment::~Segment() {
  shutdown();
#ifndef NTRACE
  printf("%s\n", __PRETTY_FUNCTION__);
#endif // ndef NTRACE
}

void Segment::shutdown() {
  if (fd != -1) {
#ifndef NTRACE
    printf("%s: fd=%d\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
    fd = -1;
  }
  cache_entry.close_for_writing();
}

void Segment::record_bytes_in(uint64_t bytes) {
  assert(!is_shutdown());
  assert(bytes <= remaining_space);
  next_stream_pos += bytes;
  remaining_space -= bytes;
  cache_entry.extend(bytes);
  if (remaining_space == 0) {
    shutdown();
  }
}

}
