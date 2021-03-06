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



#ifndef REAL_WORLD_H
#define REAL_WORLD_H

#include "Paxos/OutsideWorld.h"
#include "Pipeline/Peer/Target.h"
#include "Epoll.h"
#include "Pipeline/Client/ChosenStreamContentHandler.h"
#include "Command/NodeIdGenerationHandler.h"
#include "Pipeline/NodeName.h"

class RealWorld : public Paxos::OutsideWorld, public Epoll::ClockCache {
  RealWorld           (const RealWorld&) = delete; // no copying
  RealWorld &operator=(const RealWorld&) = delete; // no assignment

  Paxos::instant current_time      = std::chrono::steady_clock::now();
  Paxos::instant next_wake_up_time = std::chrono::steady_clock::now();

  std::vector<Pipeline::Client::ChosenStreamContentHandler*> chosen_stream_content_handlers;

  const Pipeline::NodeName     &node_name;
        Pipeline::SegmentCache &segment_cache;
  std::vector<std::unique_ptr<Pipeline::Peer::Target>> &targets;

  Command::NodeIdGenerationHandler *node_id_generation_handler = NULL;
  int log_fd = -1;
  void write_log_line(std::ostringstream&);
  void record_non_stream_content_acceptance(const Paxos::Proposal&);

public:
  RealWorld(const Pipeline::NodeName&,
                  Pipeline::SegmentCache&,
                  std::vector<std::unique_ptr<Pipeline::Peer::Target>>&);

  ~RealWorld();

  void set_node_id_generation_handler(Command::NodeIdGenerationHandler*);

  void add_chosen_value_handler(Pipeline::Client::ChosenStreamContentHandler *handler);

  void seek_votes_or_catch_up(const Paxos::Slot &first_unchosen_slot,
                              const Paxos::Term &min_acceptable_term) override;

  void offer_vote(const Paxos::NodeId &destination,
                  const Paxos::Term   &min_acceptable_term) override;

  void offer_catch_up(const Paxos::NodeId &destination) override;

  void request_catch_up(const Paxos::NodeId &destination) override;

  void send_catch_up(
    const Paxos::NodeId&          destination,
    const Paxos::Slot&            first_unchosen_slot,
    const Paxos::Era&             current_era,
    const Paxos::Configuration&   current_configuration,
    const Paxos::NodeId&          next_generated_node_id,
    const Paxos::Value::StreamName& current_stream,
    const uint64_t                current_stream_pos) override;

  void prepare_term(const Paxos::Term &term) override;

  void record_promise(const Paxos::Term &t, const Paxos::Slot &s) override;

  void make_promise(const Paxos::Promise &promise) override;

  void proposed_and_accepted(const Paxos::Proposal &proposal) override;

  void accepted(const Paxos::Proposal &proposal) override;

  void chosen_stream_content(const Paxos::Proposal &proposal) override;

  void chosen_non_contiguous_stream_content
        (const Paxos::Proposal &proposal,
         uint64_t expected_stream_pos,
         uint64_t actual_stream_pos) override;

  void chosen_unknown_stream_content
        (const Paxos::Proposal &proposal,
         Paxos::Value::StreamName expected_stream,
         uint64_t               first_stream_pos) override;

  void chosen_generate_node_ids(const Paxos::Proposal &p, Paxos::NodeId n) override;

  void chosen_new_configuration
            (const Paxos::Proposal      &proposal,
             const Paxos::Era           &era,
             const Paxos::Configuration &conf) override;

  const Paxos::instant get_current_time() override;

  void set_current_time(const Paxos::instant &t) override;

  const Paxos::instant get_next_wake_up_time() const;

  void set_next_wake_up_time(const Paxos::instant &t) override;
};

#endif // ndef REAL_WORLD_H
