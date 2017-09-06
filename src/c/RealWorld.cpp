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
#include <unistd.h>

RealWorld::RealWorld(
      const std::string &cluster_name,
      const Paxos::NodeId node_id)
  : cluster_name(cluster_name),
    node_id(node_id) {

  ensure_directory(".", "data");

  char path[PATH_MAX];
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s",
          cluster_name.c_str()));
  ensure_directory("data", path);

  char parent[PATH_MAX];
  strncpy(parent, path, PATH_MAX);
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x",
          cluster_name.c_str(), node_id));
  ensure_directory(parent, path);

  strncpy(parent, path, PATH_MAX);
  ensure_length(snprintf(path, PATH_MAX,
          "data/clu_%s/n_%08x/n_%08x.log",
          cluster_name.c_str(), node_id, node_id));

  log_fd = open(path, O_WRONLY | O_CREAT, 0644);
  if (log_fd == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: open(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }
}

RealWorld::~RealWorld() {
  if (log_fd != -1) {
    close(log_fd);
    log_fd = -1;
  }
}

void RealWorld::seek_votes_or_catch_up(
      const Paxos::Slot &first_unchosen_slot,
      const Paxos::Term &min_acceptable_term) {
  //TODO
}

void RealWorld::offer_vote(
      const Paxos::NodeId &destination,
      const Paxos::Term   &min_acceptable_term) {
  //TODO
}

void RealWorld::offer_catch_up(const Paxos::NodeId &destination) {
  //TODO
}

void RealWorld::request_catch_up(const Paxos::NodeId &destination) {
  //TODO
}

void RealWorld::send_catch_up(
      const Paxos::NodeId&            destination,
      const Paxos::Slot&              first_unchosen_slot,
      const Paxos::Era&               current_era,
      const Paxos::Configuration&     current_configuration,
      const Paxos::NodeId&            next_generated_node_id,
      const Paxos::Value::StreamName& current_stream,
      const uint64_t                  current_stream_pos) {
  //TODO
}

void RealWorld::prepare_term(const Paxos::Term &term) {
  //TODO
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
  //TODO
}

void RealWorld::proposed_and_accepted(const Paxos::Proposal &proposal) {
  //TODO
}

void RealWorld::accepted(const Paxos::Proposal &proposal) {
  //TODO
}

void RealWorld::chosen_stream_content(const Paxos::Proposal &proposal) {
  //TODO
}

void RealWorld::chosen_non_contiguous_stream_content
      (const Paxos::Proposal &proposal,
       uint64_t expected_stream_pos,
       uint64_t actual_stream_pos) {
  //TODO
}

void RealWorld::chosen_unknown_stream_content
      (const Paxos::Proposal &proposal,
       Paxos::Value::StreamName expected_stream,
       uint64_t               first_stream_pos) {
  //TODO
}

void RealWorld::chosen_generate_node_ids(const Paxos::Proposal &p, Paxos::NodeId n) {
  //TODO
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

  std::ostringstream os;

  os << "configuration changed to era "
     << std::hex << std::setfill('0') << std::setw(8) << era
     << " at slot "
     << std::hex << std::setfill('0') << std::setw(16) << proposal.slots.start()
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
