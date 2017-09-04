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



#ifndef PAXOS_PALLADIUM_H
#define PAXOS_PALLADIUM_H

#include "Paxos/Promise.h"
#include "Paxos/Proposal.h"

#include <map>
#include <algorithm>

namespace Paxos {

/*
 * A Palladium is a device believed to ensure safety.  This class ensures the
 * invariants of the Paxos algorithm hold, from which can be derived the safety
 * property that any two values chosen for the same slots are equal.
 */

struct Palladium {
  Palladium           (const Palladium&) = delete; // no copying
  Palladium &operator=(const Palladium&) = delete; // no assignment

private:

  /* Tracks proposals activated by the proposer. */
  struct ActiveSlotState {
    Value            value;
    Term             term;
    SlotRange        slots;

    std::set<NodeId> promises;
    bool             has_proposed_value;

    bool             has_accepted_value;
    Term             max_accepted_term;
    Value            max_accepted_term_value;
  };

  struct AcceptancesFromAcceptor {
    NodeId                acceptor;
    Configuration::Weight weight;
    std::vector<Proposal> proposals;
  };

  static const bool
    search_for_quorums(std::vector<AcceptancesFromAcceptor>::const_iterator,
                 const std::vector<AcceptancesFromAcceptor>::const_iterator&,
                       Proposal&,
                       Configuration::Weight,
                 const Configuration::Weight);

  NodeId _node_id;
  Slot   first_unchosen_slot;

  /* Acceptor *****************************************************/
  Term min_acceptable_term;
  std::vector<Proposal> sent_acceptances;

  /* Proposer *****************************************************/
  Slot first_inactive_slot;
  Term current_term;

  bool             is_ready_to_propose = false;
  std::set<NodeId> promises_for_inactive_slots;

  std::map<Era, Configuration> configurations;
  /* NB this is only consulted when handling promises, so not
   * on the critical path */

  std::vector<ActiveSlotState> active_slot_states;
  /* Each ActiveSlotState is nonempty, and they are in order, contiguous and
  * disjoint. */
  /* Also they run from first_unchosen_slot to first_inactive_slot. */
  /* **OR** there is a single ActiveSlotState whose .slots is empty
   * (and first_unchosen_slot == first_inactive_slot.). */

  /* Learner ******************************************************/
  /* Configuration of the first unchosen slot. */
  Era           current_era;
  Configuration current_configuration;

  std::vector<AcceptancesFromAcceptor> received_acceptances;
  /* Invariant: All contained accepted messages have start slot >=
   * first_unchosen_slot */
  /* Invariant: All contained accepted messages are for nonempty slot
   * ranges, or they are all for empty slot ranges and have
   * .proposal >= .min_acceptable_term
   */

  /* Find the maximum proposal ID for which the first-unchosen slot
   * has been accepted. */
  const Proposal *find_maximum_acceptance(Slot &promise_end_slot) const;

  void split_active_slot_states_at(const Slot slot);
  void record_current_configuration();

  const bool check_for_quorums(Proposal &chosen_message) const {

    auto total_weight = current_configuration.total_weight();
    if (total_weight == 0) { return false; }

    for (auto acceptor_iterator  = received_acceptances.cbegin();
              acceptor_iterator != received_acceptances.cend();
            ++acceptor_iterator) {

      auto accepted_weight = acceptor_iterator->weight;
      if (accepted_weight == 0) {
        continue;
      }

      for (auto &accepted_message : acceptor_iterator->proposals) {
        if (accepted_message.slots.start() != first_unchosen_slot) {
          continue;
        }
        if (accepted_message.slots.is_empty()) {
          continue;
        }
        if (accepted_message.term.era + 1 < current_era) {
          continue;
        }

        chosen_message = {
          .slots = accepted_message.slots,
          .term  = accepted_message.term,
          .value = accepted_message.value,
        };

        if (is_reconfiguration(chosen_message.value.type)) {
          // Can only choose one value if it is an reconfiguration,
          // as the subsequent values have different configurations.
          chosen_message.slots
              .set_end(chosen_message.slots.start() + 1);
        }

        if (search_for_quorums(acceptor_iterator,
                               received_acceptances.cend(),
                               chosen_message,
                               accepted_weight,
                               total_weight)) {
          return true;
        }
      }
    }

    return false;
  }

  void handle_chosen(const Proposal &chosen_message) {

    const auto &slot = chosen_message.slots.end();
    assert(first_unchosen_slot < slot);

    if (is_reconfiguration(chosen_message.value.type)) {
      assert(slot == first_unchosen_slot + 1);

      configurations.clear();
      record_current_configuration();

      const auto &a = chosen_message.value.payload.reconfiguration;

      switch (chosen_message.value.type) {
        case Value::Type::reconfiguration_inc:
          current_configuration.increment_weight(a.subject);
          break;

        case Value::Type::reconfiguration_dec:
          current_configuration.decrement_weight(a.subject);
          break;

        case Value::Type::reconfiguration_mul:
          current_configuration.multiply_weights(a.factor);
          break;

        case Value::Type::reconfiguration_div:
          current_configuration.divide_weights(a.factor);
          break;

        default:
          assert(false);
      }

      current_era += 1;
      record_current_configuration();
    }

    update_first_unchosen_slot(slot);
  }

  void update_first_unchosen_slot(const Slot &slot) {
    assert(first_unchosen_slot < slot);

    first_unchosen_slot = slot;
    if (first_inactive_slot < slot) {
      first_inactive_slot = slot;
    }

    for (auto &p : sent_acceptances) {
      p.slots.truncate(slot);
    }
    sent_acceptances.erase(
      std::remove_if(sent_acceptances.begin(),
                     sent_acceptances.end(),
                     [](const Proposal &p)
                        { return p.slots.is_empty(); }),
                     sent_acceptances.end());
      assert_sent_acceptances_valid();

    for (auto &a : active_slot_states) {
      a.slots.truncate(slot);
    }
    active_slot_states.erase(
      std::remove_if(active_slot_states.begin(),
                     active_slot_states.end(),
                     [](const ActiveSlotState &a)
                       { return a.slots.is_empty(); }),
                     active_slot_states.end());
    assert_active_slot_states_valid();

    for (auto &from_acceptor : received_acceptances) {
      auto &received_from_acceptor = from_acceptor.proposals;
      for (auto &accepted_message : received_from_acceptor) {
        accepted_message.slots.truncate(slot);
      }
      received_from_acceptor.erase(
        std::remove_if(received_from_acceptor.begin(),
                       received_from_acceptor.end(),
                       [](const Proposal &a) { return a.slots.is_empty(); }),
                       received_from_acceptor.end());
    }
  }

  void assert_sent_acceptances_valid() {
    assert(all_of(sent_acceptances.cbegin(),
                  sent_acceptances.cend(),
                  [this](const Proposal &p)
                  { return first_unchosen_slot <= p.slots.start(); }));
    assert(sent_acceptances.size() == 1
        || all_of(sent_acceptances.cbegin(),
                  sent_acceptances.cend(),
                  [](const Proposal &p)
                  { return p.slots.is_nonempty(); }));
  }

  void assert_active_slot_states_valid() {

    assert(first_unchosen_slot <= first_inactive_slot);

    assert(all_of(active_slot_states.cbegin(),
                  active_slot_states.cend(),
                  [this](const ActiveSlotState &a)
                  { return first_unchosen_slot <= a.slots.start()
                        && a.slots.start()     <= a.slots.end()
                        && a.slots.end()       <= first_inactive_slot; }));

    assert(active_slot_states.size() == 1
        || all_of(active_slot_states.cbegin(),
                  active_slot_states.cend(),
                  [](const ActiveSlotState &a)
                  { return a.slots.is_nonempty(); }));

    assert(active_slot_states.size() == 0
        || active_slot_states.cbegin()->slots.start()
        == first_unchosen_slot);

    assert(active_slot_states.size() == 0
        || active_slot_states.crbegin()->slots.end()
        == first_inactive_slot);

#ifndef NDEBUG
    for (auto curr  = active_slot_states.cbegin();
              curr != active_slot_states.cend();
              curr++) {
      auto next = curr;
      next++;
      if (next != active_slot_states.cend()) {
        assert(curr->slots.end() == next->slots.start());
      }
    }

#endif
  }

public:
  Palladium(const NodeId, const Slot,
            const Era, const Configuration&);

  const Term &get_min_acceptable_term() const
    { return min_acceptable_term; }

  const Term &next_activated_term() const
    { return current_term; }

  const Slot &next_activated_slot() const
    { return first_inactive_slot; }

  const bool &activation_will_yield_proposals() const
    { return is_ready_to_propose; }

  const NodeId &node_id() const { return _node_id; }

  std::ostream& write_to(std::ostream &) const;

  const Promise handle_prepare(const Term&);
  const Proposal handle_promise(const NodeId, const Promise&);

  /* Activates the next `count` slots with the given value. */
  const Proposal activate(const Value &value, const uint64_t count) {

    Proposal proposal = {
        .slots = SlotRange(first_inactive_slot,
                           first_inactive_slot + count),
        .term  = current_term,
        .value = value
      };
    first_inactive_slot = proposal.slots.end();

    auto last_active_slot = active_slot_states.rbegin();

    if   (last_active_slot != active_slot_states.rend()
      &&  last_active_slot->term               == current_term
      &&  last_active_slot->value              == value
      &&  last_active_slot->promises           == promises_for_inactive_slots
      &&  last_active_slot->has_proposed_value == is_ready_to_propose
      && !last_active_slot->has_accepted_value) {

      last_active_slot->slots.set_end(first_inactive_slot);
      assert_active_slot_states_valid();

      if (!is_ready_to_propose) {
        proposal.slots.set_end(proposal.slots.start());
      }
      return proposal;
    }

    if (count == 0) {
      return proposal;
    }

    // Special case: the first element of active_slot_states is for an
    // empty set of slots. This means there are no other elements; remove
    // it, to be replaced with a nonempty state.
    auto it = active_slot_states.begin();
    if (it != active_slot_states.end()
        && it->slots.is_empty()) {
      active_slot_states.clear();
    }

    active_slot_states.push_back({
        .value              = value,
        .term               = current_term,
        .slots              = proposal.slots,
        .promises           = promises_for_inactive_slots,
        .has_proposed_value = is_ready_to_propose,
        .has_accepted_value = false,
      });

    if (!is_ready_to_propose) {
      proposal.slots.set_end(proposal.slots.start());
    }

    assert_active_slot_states_valid();
    return proposal;
  }

  /* Returns whether the proposal was accepted or not. */
  const bool handle_proposal(const Proposal &proposal) {
    if (proposal.term < min_acceptable_term) {
      return false;
    }

    auto effective_slots = proposal.slots;
    effective_slots.truncate(first_unchosen_slot);

    if (effective_slots.is_empty()) {
      return false;
    }

    for (auto it  = sent_acceptances.begin();
              it != sent_acceptances.end();
              it++) {

      Proposal &p = *it;

      if (p.value == proposal.value
       && p.term  == proposal.term
       && p.slots.can_extend_with(effective_slots)) {

        p.slots.extend_with(effective_slots);
        assert_sent_acceptances_valid();
        return true;
      }
    }

    sent_acceptances.push_back({
          .slots = effective_slots,
          .term  = proposal.term,
          .value = proposal.value
        });

    assert_sent_acceptances_valid();
    return true;
  }

  const void handle_accepted
    (const NodeId acceptor,
     const Proposal &accepted_message) {

    if (accepted_message.term.era + 1 < current_era) {
      return;
    }

    auto effective_slots = accepted_message.slots;
    effective_slots.truncate(first_unchosen_slot);

    if (effective_slots.is_empty()) {
      return;
    }

    for (auto received_acceptance_it  = received_acceptances.begin();
              received_acceptance_it != received_acceptances.end();
              received_acceptance_it++) {

      auto &received_acceptance = *received_acceptance_it;
      if (received_acceptance.acceptor != acceptor) {
        continue;
      }

      auto accepted_message_copy = accepted_message;
      accepted_message_copy.slots = effective_slots;
      received_acceptance.proposals.push_back(accepted_message_copy);
      return;
    }

    const auto &conf_entries = current_configuration.entries;
    auto conf_entry = find_if(
      conf_entries.begin(),
      conf_entries.end(),
      [&acceptor](const Configuration::Entry &entry) {
        return entry.node_id() == acceptor;
      });

    if (conf_entry != conf_entries.end()
        && conf_entry->weight() > 0) {
      auto accepted_message_copy = accepted_message;
      accepted_message_copy.slots = effective_slots;
      received_acceptances.push_back({
        .acceptor  = acceptor,
        .weight    = conf_entry->weight(),
        .proposals = std::vector<Proposal>(1, accepted_message_copy)
      });
    }
  }

  const Proposal check_for_chosen_slots() {
    Proposal chosen_message = {
      .slots = SlotRange(first_unchosen_slot,
                         first_unchosen_slot),
      .term  = current_term,
      .value = {
        .type = Value::Type::no_op
      }
    };

    if (check_for_quorums(chosen_message)) {
      handle_chosen(chosen_message);
    } else {
      chosen_message.slots.set_end(first_unchosen_slot);
    }
    return chosen_message;
  }

};
std::ostream& operator<<(std::ostream&, const Palladium&);

}

#endif // ndef PAXOS_PALLADIUM_H
