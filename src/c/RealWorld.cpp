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



#include "RealWorld.h"
#include "directories.h"
#include <limits.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <sstream>
#include <iomanip>

RealWorld::RealWorld(
      const Pipeline::NodeName &node_name,
      Pipeline::SegmentCache &segment_cache,
      std::vector<std::unique_ptr<Pipeline::Peer::Target>> &targets)
  : node_name(node_name),
    segment_cache(segment_cache),
    targets(targets) {

  ensure_directory(".", "data");

  char path[PATH_MAX];
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s",
          node_name.cluster.c_str()));
  ensure_directory("data", path);

  char parent[PATH_MAX];
  strncpy(parent, path, PATH_MAX);
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x",
          node_name.cluster.c_str(), node_name.id));
  ensure_directory(parent, path);

  strncpy(parent, path, PATH_MAX);
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x/n_%08x.log",
          node_name.cluster.c_str(), node_name.id, node_name.id));

  log_fd = open(path, O_WRONLY | O_CREAT, 0644);
  if (log_fd == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: open(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }

  sync_directory(parent);
}

RealWorld::~RealWorld() {
  if (log_fd != -1) {
    close(log_fd);
    log_fd = -1;
  }
}

void RealWorld::add_chosen_value_handler(Pipeline::Client::ChosenStreamContentHandler *handler) {
  chosen_stream_content_handlers.push_back(handler);
}

void RealWorld::seek_votes_or_catch_up(
      const Paxos::Slot &first_unchosen_slot,
      const Paxos::Term &min_acceptable_term) {
  for (auto &target : targets) {
    target->seek_votes_or_catch_up(first_unchosen_slot, min_acceptable_term);
  }
}

void RealWorld::offer_vote(
      const Paxos::NodeId &destination,
      const Paxos::Term   &min_acceptable_term) {
  for (auto &target : targets) {
    target->offer_vote(destination, min_acceptable_term);
  }
}

void RealWorld::offer_catch_up(const Paxos::NodeId &destination) {
  for (auto &target : targets) {
    target->offer_catch_up(destination);
  }
}

void RealWorld::request_catch_up(const Paxos::NodeId &destination) {
  for (auto &target : targets) {
    target->request_catch_up(destination);
  }
}

void RealWorld::send_catch_up(
      const Paxos::NodeId&            destination,
      const Paxos::Slot&              first_unchosen_slot,
      const Paxos::Era&               current_era,
      const Paxos::Configuration&     current_configuration,
      const Paxos::NodeId&            next_generated_node_id,
      const Paxos::Value::StreamName& current_stream,
      const uint64_t                  current_stream_pos) {

  for (auto &target : targets) {
    target->send_catch_up(destination,
                         first_unchosen_slot,
                         current_era,
                         current_configuration,
                         next_generated_node_id,
                         current_stream,
                         current_stream_pos);
  }
}

void RealWorld::prepare_term(const Paxos::Term &term) {
  for (auto &target : targets) {
    target->prepare_term(term);
  }
}

void RealWorld::write_log_line(std::ostringstream &os) {
  std::string oss = os.str();
  const char *buf = oss.c_str();
  size_t bytes_to_write = oss.length();

  while (bytes_to_write > 0) {
    ssize_t write_result = write(log_fd, buf, bytes_to_write);
    if (write_result == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: write() failed\n",
        __PRETTY_FUNCTION__);
      abort();
    } else {
      assert(0 < write_result);
      size_t bytes_written = write_result;
      assert(bytes_written <= bytes_to_write);
      bytes_to_write -= bytes_written;
      buf += bytes_written;
    }
  }

  int fsync_result = fsync(log_fd);
  if (fsync_result == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: fsync() failed\n",
      __PRETTY_FUNCTION__);
    abort();
  }
}

void RealWorld::record_promise(const Paxos::Term &t, const Paxos::Slot &s) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__
    << " term=" << t
    << " slot=" << s
    << std::endl;
#endif

  std::ostringstream os;
  os << "promise "
     << std::hex << std::setfill('0') << std::setw(8) << t.era         << '.'
     << std::hex << std::setfill('0') << std::setw(8) << t.term_number << '.'
     << std::hex << std::setfill('0') << std::setw(8) << t.owner
     << " at slot "
     << std::hex << std::setfill('0') << std::setw(16) << s
     << " made" << std::endl;

  write_log_line(os);
}

void RealWorld::make_promise(const Paxos::Promise &promise) {
  for (auto &target : targets) {
    target->make_promise(promise);
  }
}

void RealWorld::record_non_stream_content_acceptance(const Paxos::Proposal &proposal) {

  const auto &slots = proposal.slots;
  const auto &term = proposal.term;
  const auto &payload = proposal.value.payload;

  std::ostringstream os;
  os << "proposal accepted for slots ["
     << std::hex << std::setfill('0') << std::setw(16) << slots.start() << ','
     << std::hex << std::setfill('0') << std::setw(16) << slots.end() << ')'
     << " at term "
     << std::hex << std::setfill('0') << std::setw(8) << term.era         << '.'
     << std::hex << std::setfill('0') << std::setw(8) << term.term_number << '.'
     << std::hex << std::setfill('0') << std::setw(8) << term.owner
     << ": ";

  switch (proposal.value.type) {
    case Paxos::Value::Type::no_op:
      os << "no-op";
      break;
    case Paxos::Value::Type::generate_node_id:
      os << "generate-node-id "
         << std::hex << std::setfill('0') << std::setw(8)
         << payload.originator;
      break;
    case Paxos::Value::Type::reconfiguration_inc:
      os << "reconfiguration_inc "
         << std::hex << std::setfill('0') << std::setw(8)
         << payload.reconfiguration.subject;
      break;
    case Paxos::Value::Type::reconfiguration_dec:
      os << "reconfiguration_dec "
         << std::hex << std::setfill('0') << std::setw(8)
         << payload.reconfiguration.subject;
      break;
    case Paxos::Value::Type::reconfiguration_mul:
      os << "reconfiguration_mul "
         << std::hex << std::setfill('0') << std::setw(2)
         << ((uint32_t)payload.reconfiguration.factor);
      break;
    case Paxos::Value::Type::reconfiguration_div:
      os << "reconfiguration_div "
         << std::hex << std::setfill('0') << std::setw(2)
         << ((uint32_t)payload.reconfiguration.factor);
      break;
    default:
      fprintf(stderr, "%s: unexpected proposal type: %d\n",
        __PRETTY_FUNCTION__, proposal.value.type);
      abort();
  }

  os << std::endl;
  write_log_line(os);
}

void RealWorld::proposed_and_accepted(const Paxos::Proposal &proposal) {
  if (proposal.value.type != Paxos::Value::Type::stream_content) {
    record_non_stream_content_acceptance(proposal);
  }

  for (auto &target : targets) {
    target->proposed_and_accepted(proposal);
  }
}

void RealWorld::accepted(const Paxos::Proposal &proposal) {
  if (proposal.value.type != Paxos::Value::Type::stream_content) {
    record_non_stream_content_acceptance(proposal);
  }

  for (auto &target : targets) {
    target->accepted(proposal);
  }
}

void RealWorld::chosen_stream_content(const Paxos::Proposal &proposal) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__
    << " proposal=" << proposal
    << std::endl;
#endif

  segment_cache.expire_because_chosen_to(proposal.slots.end());

  for (auto h : chosen_stream_content_handlers) {
    h->handle_stream_content(proposal);
  }
}

void RealWorld::chosen_non_contiguous_stream_content
      (const Paxos::Proposal &proposal,
       uint64_t expected_stream_pos,
       uint64_t actual_stream_pos) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__
    << " proposal=" << proposal
    << " expected=" << expected_stream_pos
    << " actual="   << actual_stream_pos
    << std::endl;
#endif

  segment_cache.expire_because_chosen_to(proposal.slots.end());

  for (auto h : chosen_stream_content_handlers) {
    h->handle_non_contiguous_stream_content(proposal);
  }
}

void RealWorld::chosen_unknown_stream_content
      (const Paxos::Proposal &proposal,
       Paxos::Value::StreamName expected_stream,
       uint64_t               first_stream_pos) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__
    << " proposal=" << proposal
    << " expected=" << expected_stream
    << " first_stream_pos=" << first_stream_pos
    << std::endl;
#endif

  segment_cache.expire_because_chosen_to(proposal.slots.end());

  for (auto h : chosen_stream_content_handlers) {
    h->handle_unknown_stream_content(proposal);
  }
}

void RealWorld::chosen_generate_node_ids(const Paxos::Proposal &p, Paxos::NodeId n) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__
    << " proposal=" << p
    << " node_id="  << n
    << std::endl;
#endif

  assert(p.value.type == Paxos::Value::Type::generate_node_id);
  assert(p.value.payload.originator == node_name.id);

  segment_cache.expire_because_chosen_to(p.slots.end());

  if (node_id_generation_handler != NULL) {
    for (Paxos::Slot s = p.slots.start();
                     s < p.slots.end();
                     s++) {
      node_id_generation_handler->handle_node_id_generation(s, n++);
    }
  }
}

void RealWorld::chosen_new_configuration
          (const Paxos::Proposal      &proposal,
           const Paxos::Era           &era,
           const Paxos::Configuration &conf) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__
    << " proposal=" << proposal
    << " era="      << era
    << " conf="     << conf
    << std::endl;
#endif

  segment_cache.expire_because_chosen_to(proposal.slots.end());

  Paxos::Slot slot = proposal.slots.start();
  assert(proposal.slots.end() == slot + 1);

  std::ostringstream os;

  os << "configuration changed to era "
     << std::hex << std::setfill('0') << std::setw(8) << era
     << " at slot "
     << std::hex << std::setfill('0') << std::setw(16) << slot
     << ':';

  for (const auto &e : conf.entries) {
    os << ' '
       << std::hex << std::setfill('0') << std::setw(8) << e.node_id()
       << '='
       << std::hex << std::setfill('0') << std::setw(2) << ((uint32_t)e.weight());
  }
  os << std::endl;

  write_log_line(os);
}

const Paxos::instant RealWorld::get_current_time() {
  return std::chrono::steady_clock::now();
}

const Paxos::instant RealWorld::get_next_wake_up_time() const {
  return next_wake_up_time;
}

void RealWorld::set_next_wake_up_time(const Paxos::instant &t) {
  if (next_wake_up_time < t) {
#ifndef NTRACE
    std::cout << __PRETTY_FUNCTION__ << ": " <<
      std::chrono::time_point_cast<std::chrono::milliseconds>
        (t).time_since_epoch().count() << std::endl;
#endif // ndef NTRACE
    next_wake_up_time = t;
  }
}

void RealWorld::set_node_id_generation_handler(Command::NodeIdGenerationHandler *h) {
  assert(node_id_generation_handler == NULL);
  node_id_generation_handler = h;
}
