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



#ifndef PAXOS_LEGISLATOR_H
#define PAXOS_LEGISLATOR_H

#include "Paxos/Palladium.h"
#include "Paxos/OutsideWorld.h"

using delay   = std::chrono::steady_clock::duration;

namespace Paxos {

class Legislator {
  Legislator           (const Legislator&) = delete; // no copying
  Legislator &operator=(const Legislator&) = delete; // no assignment

  public:
    enum Role {
      candidate,
      follower,
      leader,
      incumbent
    };

  private:
    OutsideWorld &_world;
    Palladium     _palladium;

    instant   _next_wake_up      = _world.get_current_time();
    Role      _role              = Role::candidate;
    NodeId    _leader_id         = 0; /* Not relevant if a candidate. */
    delay     _incumbent_timeout = std::chrono::milliseconds(100);
    delay     _leader_timeout    = std::chrono::milliseconds(8000);
    delay     _follower_timeout  = std::chrono::milliseconds(9000);

    const int _minimum_retry_delay_ms   = 150;
    const int _maximum_retry_delay_ms   = 60000;
    const int _retry_delay_increment_ms = 150;
          int _retry_delay_ms           = _retry_delay_increment_ms;

    std::set<NodeId> _offered_votes;
    bool             _seeking_votes = false;
    Term             _minimum_term_for_peers;
    Term             _attempted_term;

    /* RSM state */
    NodeId            _next_generated_node_id = 2;
    Value::StreamName _current_stream = {.owner = 0, .id = 0};
    uint64_t          _current_stream_pos     = 0;

    const bool is_leading() const {
      return _role == Role::leader || _role == Role::incumbent;
    }

    void set_next_wake_up_time(const instant &t) {
      _next_wake_up = t;
      _world.set_next_wake_up_time(_next_wake_up);
    }

    std::chrono::steady_clock::duration random_retry_delay();

  public:
    Legislator( OutsideWorld&,
          const NodeId&,
          const Slot&,
          const Era&,
          const Configuration&);

    std::ostream &write_to(std::ostream&) const;

    void handle_wake_up();
    void handle_seek_votes_or_catch_up
      (const NodeId&, const Slot&, const Term&);
    void handle_offer_vote(const NodeId&, const Term&);
    void handle_offer_catch_up(const NodeId&);

    void abdicate_to(const NodeId&);
    void start_term(const NodeId&);
    void handle_prepare_term(const NodeId&, const Term&);
    void handle_promise(const NodeId&, const Promise&);

    void activate_slots(const Value &value, const uint64_t count) {
      if (_role == Role::follower) { return; }
      handle_proposal(_palladium.activate(value, count), true);
    }

    void handle_proposed_and_accepted(const NodeId   &sender,
                                      const Proposal &proposal) {
      handle_proposal(proposal, false);
      handle_accepted(sender, proposal);
    }

  private:
    void handle_proposal(const Proposal &proposal, bool send_proposal) {
      if (proposal.slots.is_empty()) { return; }
      if (!_palladium.handle_proposal(proposal)) { return; }

      if (send_proposal) {
        _world.proposed_and_accepted(proposal);
      } else {
        _world.accepted(proposal);
      }

      handle_accepted(_palladium.node_id(), proposal);
    }

  public:
    void handle_accepted(const NodeId &sender, const Proposal &proposal) {
      if (proposal.slots.is_empty()) { return; }
      _palladium.handle_accepted(sender, proposal);

      bool nothing_chosen = true;
      for (Proposal chosen = _palladium.check_for_chosen_slots();
                    chosen.slots.is_nonempty();
                    chosen = _palladium.check_for_chosen_slots()) {
        nothing_chosen = false;
        if (_leader_id != chosen.term.owner) {
          printf("Leader changed to node %u\n", chosen.term.owner);
        }
        _leader_id = chosen.term.owner;
        uint64_t chosen_slot_count = chosen.slots.end() - chosen.slots.start();
        auto &payload = chosen.value.payload;

        if (LIKELY(chosen.value.type == Value::Type::stream_content)) {
          uint64_t first_written_stream_pos
            = chosen.slots.start() - chosen.value.payload.stream.offset;

          if (LIKELY(payload.stream.name.owner == _current_stream.owner
                  && payload.stream.name.id    == _current_stream.id)) {

            if (LIKELY(_current_stream_pos == first_written_stream_pos)) {
              _current_stream_pos += chosen_slot_count;
              _world.chosen_stream_content(chosen);
            } else {
              _world.chosen_non_contiguous_stream_content
                  (chosen,
                   _current_stream_pos,
                   first_written_stream_pos);

              _current_stream_pos = 0; // Need a new stream.
            }

          } else {

            if (first_written_stream_pos == 0) {
              _current_stream     = payload.stream.name;
              _current_stream_pos = chosen_slot_count;
              _world.chosen_stream_content(chosen);
            } else {
              _world.chosen_unknown_stream_content
                  (chosen, _current_stream,
                   first_written_stream_pos);
            }
          }

        } else {
          switch(chosen.value.type) {
            case (Value::Type::generate_node_id):
              if (payload.originator == _palladium.node_id()) {
                _world.chosen_generate_node_ids(chosen,
                                                _next_generated_node_id);
              }
              _next_generated_node_id += chosen_slot_count;
              break;

            case (Value::Type::no_op):
              break;

            default:
              assert(chosen_slot_count == 1);
              assert(is_reconfiguration(chosen.value.type));
              _world.chosen_new_configuration
                (chosen,
                _palladium.get_current_era(),
                _palladium.get_current_configuration());
              break;
          }
        }
      }

      if (nothing_chosen) { return; }

      _seeking_votes = false;
      _offered_votes.clear();

      instant now = _world.get_current_time();
      if (_leader_id == _palladium.node_id()) {
        if (!is_leading()) {
          std::cout << "This node became leader" << std::endl;
        }
        _role = Role::leader;
        set_next_wake_up_time(now + _leader_timeout);
      } else {
        if (_role != Role::follower) {
          std::cout << "This node became a follower of " << _leader_id << std::endl;
          _role = Role::follower;
        }
        set_next_wake_up_time(now + _follower_timeout);
      }
    }

};
std::ostream& operator<<(std::ostream&, const Legislator&);

}

#endif // ndef PAXOS_LEGISLATOR_H
