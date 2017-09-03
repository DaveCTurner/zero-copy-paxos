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

    SlotRange activated_slots(first_inactive_slot, first_inactive_slot + count);

    Proposal proposal = {
        .slots = SlotRange(first_inactive_slot,
                           first_inactive_slot),
        .term  = current_term,
        .value = value
      };

    if (count == 0) {
      return proposal;
    }

    first_inactive_slot = activated_slots.end();

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
        .slots              = activated_slots
      });

    return proposal;
  }
};
std::ostream& operator<<(std::ostream&, const Palladium&);

}

#endif // ndef PAXOS_PALLADIUM_H
