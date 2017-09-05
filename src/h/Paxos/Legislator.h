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

/* The Legislator is the layer between the Palladium and the outside world.
The Palladium is responsible for safety; the legislator acts as an adapter
between the pure Paxos messages used in the Palladium and the ones passed
between nodes. It is also responsible for the liveness properties of the
system. The terminology is taken directly from the Part Time Parliament
paper.*/

class Legislator {
  Legislator           (const Legislator&) = delete; // no copying
  Legislator &operator=(const Legislator&) = delete; // no assignment

  public:

    enum Role {
      /* No known leader: no slot is known to have been chosen, or the
         last-known chosen slot was more than _follower_timeout ago.
         This node is a candidate to become leader. */
      candidate,

      /* Some other node is known to be leader: it is the owner of the last
         slot known to be chosen, which was no more than
        _follower_timeout ago. */
      follower,

      /* This node is known to be leader: it is the owner of the last slot
         known to be chosen, which was no more than _leader_timeout ago.
      */
      leader,

      /* This node was known to be leader, but the last slot known to be
         chosen was more than _leader_timeout ago, so it is now trying to
         be re-elected by proposing another slot. */
      incumbent
    };

  private:
    OutsideWorld &_world;
    Palladium     _palladium;

    /* Timeout & role things */
    instant   _next_wake_up      = _world.get_current_time();
    Role      _role              = Role::candidate;
    NodeId    _leader_id         = 0; /* Not relevant if a candidate. */

              /* Timeout for incumbent -> candidate transition */
    delay     _incumbent_timeout = std::chrono::milliseconds(100);
              /* Timeout for leader -> incumbent transition
                 (reset when a new slot is chosen) */
    delay     _leader_timeout    = std::chrono::milliseconds(8000);
              /* Timeout for follower -> candidate transition
                 (reset when a new slot is chosen or a catch-up occurs */
    delay     _follower_timeout  = std::chrono::milliseconds(9000);

              /* Candidate wake-up interval parameters. Candidates wake
                 up after a random-length delay in [_minimum_retry_delay_ms,
                 _retry_delay_ms] where _retry_delay_ms increases by
                 _retry_delay_increment_ms up to _maximum_retry_delay_ms on
                 each failed attempt. */
    const int _minimum_retry_delay_ms   = 150;
    const int _maximum_retry_delay_ms   = 60000;
    const int _retry_delay_increment_ms = 150;
          int _retry_delay_ms           = _retry_delay_increment_ms;

    /* Re-election data */
    std::set<NodeId> _offered_votes;
    bool             _seeking_votes = false;
    Term             _minimum_term_for_peers;
    Term             _attempted_term;
    Term             _deferred_term;

    /* Restricting new eras */
    bool             _change_era_restricted_by_slot      = false;
    bool             _change_era_restricted_by_term      = false;
    Slot             _change_era_after_slot              = 0;
    Era              _change_era_after_proposal_from_era = 0;

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
    void handle_request_catch_up(const NodeId&);
    void unsafely_stage_coup();
    void handle_send_catch_up(const Slot&, const Era&, const Configuration&,
      const NodeId&, const Value::StreamName&, const uint64_t);

    void abdicate_to(const NodeId&);
    void start_term(const NodeId&);
    void handle_prepare_term(const NodeId&, const Term&);
    void handle_promise(const NodeId&, const Promise&);

    void activate_slots(const Value &value, const uint64_t count) {
      if (UNLIKELY(_role == Role::follower))        { return; }
      if (UNLIKELY(_change_era_restricted_by_slot)) { return; }
      if (UNLIKELY(_change_era_restricted_by_term)) { return; }
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

      if (UNLIKELY(_change_era_restricted_by_term
        && proposal.term.era <= _change_era_after_proposal_from_era)) {
        _change_era_restricted_by_term = false;
        assert(!_change_era_restricted_by_slot);
      }
    }

  public:
    void handle_accepted(const NodeId &sender, const Proposal &proposal) {
      if (proposal.slots.is_empty()) { return; }
      _palladium.handle_accepted(sender, proposal);

      Era old_era = _palladium.get_current_era();
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
        assert(chosen_slot_count > 0);
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

      if (UNLIKELY(old_era != _palladium.get_current_era())) {
        if (is_leading()) {
          start_term(_palladium.node_id());
        } else if (_palladium.get_min_acceptable_term()
                <= _deferred_term) {
          handle_prepare_term(_leader_id, _deferred_term);
        }
      }

      if (UNLIKELY(_change_era_restricted_by_slot
        && _change_era_after_slot
              < _palladium.next_chosen_slot())) {

        assert(!_change_era_restricted_by_term);
        _change_era_restricted_by_slot = false;
        _change_era_restricted_by_term = true;
        _change_era_after_proposal_from_era = _palladium.get_current_era();
      }

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
