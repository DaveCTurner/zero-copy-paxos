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

public:
  Palladium(const NodeId, const Slot,
            const Era, const Configuration&);

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

    sent_acceptances.push_back({
          .slots = effective_slots,
          .term  = proposal.term,
          .value = proposal.value
        });

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
};
std::ostream& operator<<(std::ostream&, const Palladium&);

}

#endif // ndef PAXOS_PALLADIUM_H
