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



#include "Paxos/Palladium.h"

namespace Paxos {

Palladium::Palladium(const NodeId         id,
                     const Slot           initial_slot,
                     const Era            initial_era,
                     const Configuration &initial_configuration)
  : _node_id                (id)
  , first_unchosen_slot     (initial_slot)
  , first_inactive_slot     (initial_slot)
  , current_era             (initial_era)
  , current_configuration   (initial_configuration) {}

/* Find the maximum term ID for which the first-unchosen slot
 * has been accepted. */
const Proposal *Palladium::find_maximum_acceptance
    (Slot &promise_end_slot) const {

  const Proposal *maximum_acceptance = NULL;

  for (const auto &p : sent_acceptances) {
    if (p.slots.contains(first_unchosen_slot)
      && (maximum_acceptance == NULL
          || maximum_acceptance->term < p.term)) {
      maximum_acceptance = &p;
    }
  }

  if (maximum_acceptance != NULL) {
    /* maximum_acceptance applies to multiple slots. It may have
     * been superseded by another acceptance for some later slots.
     * Therefore find the slot promise_end_slot such that
     * it applies to [first_unchosen_slot, promise_end_slot). */

    promise_end_slot = maximum_acceptance->slots.end();

    for (const auto &p : sent_acceptances) {

      if (/* starts after the first_unchosen_slot */
          first_unchosen_slot < p.slots.start()
       && /* nonempty */
          p.slots.is_nonempty()
       && /* for a not-earlier term */
          maximum_acceptance->term <= p.term
       && /* reduces promise_end_slot */
          p.slots.start() < promise_end_slot) {

        promise_end_slot = p.slots.start();
      }
    }
  } else {
    /* first_unchosen_slot not accepted, but some later slot may have
     * been.  Find the first such. */
    bool found_any_slots = false;
    for (const auto &p : sent_acceptances) {
      if (/* starts after the first_unchosen_slot */
          first_unchosen_slot < p.slots.start()
       && /* nonempty */
          p.slots.is_nonempty()
       && /* reduces promise_end_slot */
          (!found_any_slots
            || (p.slots.start() < promise_end_slot))) {

        promise_end_slot = p.slots.start();
        found_any_slots = true;
      }
    }
  }

  return maximum_acceptance;
}

const Promise Palladium::handle_prepare(const Term &new_term) {

  Promise promise(Promise::Type::none,
                  first_unchosen_slot,
                  first_unchosen_slot,
                  new_term);

  if (new_term < min_acceptable_term) {
    /* Conflicts with minimum acceptable proposal - ignored */
    promise.type = Promise::Type::none;
    return promise;
  }

  min_acceptable_term = new_term;

  /* Send a promise that covers the first unchosen slot. */

  if (sent_acceptances.empty()
    || sent_acceptances[0].slots.is_empty()) {
    /* Have accepted no proposals for any active slots */
    promise.type = Promise::Type::multi;
  } else {
    /* ... else have accepted some slot >= the first unchosen. */

    /* Has the first unchosen one itself been accepted? */
    Slot new_end = promise.slots.end();
    const auto *maximum_acceptance = find_maximum_acceptance(new_end);
    promise.slots.set_end(new_end);

    if (maximum_acceptance == NULL) {
      /* No, first unchosen slot has not been accepted. */

      promise.type = Promise::Type::free;

    } else if (maximum_acceptance->term < new_term) {
      /* Yes, first unchosen slot has been accepted,
         but for an earlier term than new_term. */

      promise.type                    = Promise::Type::bound;
      promise.max_accepted_term       = maximum_acceptance->term;
      promise.max_accepted_term_value = maximum_acceptance->value;

    } else {
      /* Yes, first unchosen slot has been accepted,
         and for a term no earlier than new_term, so
         no promise can be made. */

      promise.type = Promise::Type::none;
    }
  }
  return promise;
}

const Proposal Palladium::handle_promise
    (const NodeId acceptor, const Promise &promise) {

  Proposal empty_proposal = {
    .slots = {
      .start = first_unchosen_slot,
      .end   = first_unchosen_slot
    },
    .term = promise.term
  };

  if (promise.term.owner != node_id()) {
    return empty_proposal;
  }

  return empty_proposal;
}

std::ostream& operator<<(std::ostream &o, const Palladium &palladium) {
  return palladium.write_to(o);
}

std::ostream& Palladium::write_to(std::ostream &o) const {
  o << "node_id             = " << node_id()           << std::endl;
  o << "first_unchosen_slot = " << first_unchosen_slot << std::endl;
  o << "first_inactive_slot = " << first_inactive_slot << std::endl;
  o << "min_acceptable_term = " << min_acceptable_term << std::endl;
  o << "current_term        = " << current_term        << std::endl;
  o << "sent_acceptances:"                             << std::endl;
  for (const auto &a : sent_acceptances) { o << "  " << a << std::endl; }
  o << "active_slot_states:" << std::endl;
  for (const auto &a : active_slot_states) {
    o << "  " << a.term << "@" << a.slots << ": " << a.value << std::endl;
  }

  return o;
}

}
