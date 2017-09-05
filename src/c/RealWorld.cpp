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

RealWorld::RealWorld(
      const std::string &cluster_name,
      const Paxos::NodeId node_id)
  : cluster_name(cluster_name),
    node_id(node_id) {}

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

void RealWorld::record_promise(const Paxos::Term &t, const Paxos::Slot &s) {
  //TODO
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
  //TODO
}

const Paxos::instant RealWorld::get_current_time() {
  return std::chrono::steady_clock::now();
}

const Paxos::instant RealWorld::get_next_wake_up_time() const {
  return next_wake_up_time;
}

void RealWorld::set_next_wake_up_time(const Paxos::instant &t) {
  if (next_wake_up_time < t) {
    next_wake_up_time = t;
  }
}
