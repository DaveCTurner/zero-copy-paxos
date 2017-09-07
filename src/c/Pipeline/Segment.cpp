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
       (const NodeName                   &node_name,
        const Paxos::Value::OffsetStream  stream,
        const Paxos::Term                &term,
        const uint64_t                    first_stream_pos)
  : next_stream_pos (first_stream_pos)
  , remaining_space(CLIENT_SEGMENT_DEFAULT_SIZE
      - (first_stream_pos & (CLIENT_SEGMENT_DEFAULT_SIZE-1)))
  , term(term)
  , stream_offset(stream.offset) {

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
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x/own_%08x_str_%08x/off_%016lx/pos_%016lx_trm_%08x_%08x_%08x",
          node_name.cluster.c_str(), node_name.id,
          stream.name.owner,
          stream.name.id,
          stream.offset,
          next_stream_pos,
          term.era, term.term_number, term.owner));

  fd = open(path, O_CREAT | O_RDWR, 0644);
  if (fd == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: open(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }

  sync_directory(parent);
}

Segment::~Segment() {
  shutdown();
}

void Segment::shutdown() {
  if (fd != -1) {
    close(fd);
    fd = -1;
  }
}

void Segment::record_bytes_in(uint64_t bytes) {
  assert(!is_shutdown());
  assert(bytes <= remaining_space);
  next_stream_pos += bytes;
  remaining_space -= bytes;
  if (remaining_space == 0) {
    shutdown();
  }
}

}
