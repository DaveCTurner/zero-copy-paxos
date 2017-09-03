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
  , current_configuration   (initial_configuration) {
    record_current_configuration();
}

void Palladium::record_current_configuration() {
  configurations.insert(std::pair<Era, Configuration>
                    (current_era, current_configuration));

  for (auto it  = received_acceptances.begin();
            it != received_acceptances.end();
            it++) {

    const auto &conf_entries = current_configuration.entries;
    const auto entry = find_if(conf_entries.begin(),
                               conf_entries.end(),
                               [it](const Configuration::Entry &entry) {
      return entry.node_id() == it->acceptor; });

    it->weight = entry == conf_entries.end()
                       ? 0
                       : entry->weight();
  }

  received_acceptances.erase(remove_if(received_acceptances.begin(),
                                       received_acceptances.end(),
      [](const AcceptancesFromAcceptor &x) { return x.weight == 0; }),
                                       received_acceptances.end());
}

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

  assert(new_term.era <= current_era);

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

    assert(all_of(sent_acceptances.cbegin(),
                  sent_acceptances.cend(),
                  [](const Proposal &p)
                  { return p.slots.is_empty(); }));
  } else {
    /* ... else have accepted some slot >= the first unchosen. */

    /* Has the first unchosen one itself been accepted? */
    Slot new_end = promise.slots.end();
    const auto *maximum_acceptance = find_maximum_acceptance(new_end);
    promise.slots.set_end(new_end);

    assert(first_unchosen_slot == promise.slots.start());
    assert(first_unchosen_slot <  promise.slots.end());
    assert(promise.slots.is_nonempty());

    if (maximum_acceptance == NULL) {
      /* No, first unchosen slot has not been accepted. */

      assert(all_of(sent_acceptances.cbegin(),
                    sent_acceptances.cend(),
                    [new_end](const Proposal &p)
                    { return new_end <= p.slots.start();}));

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

  if (promise.type == Promise::Type::none) {
    return empty_proposal;
  }

  auto effective_slots = promise.slots;
  effective_slots.truncate(first_unchosen_slot);

  if (promise.type == Promise::Type::multi) {
    effective_slots.set_end(effective_slots.start());
  } else {
    if (effective_slots.is_empty()) {
      return empty_proposal;
    }
  }

  bool propose_first_unchosen = false;

  if (first_inactive_slot < effective_slots.end()) {

    if (first_inactive_slot == first_unchosen_slot
        && is_ready_to_propose) {

      // About to activate an slot proposer for the first_unchosen_slot
      // which has .has_proposed_value = true.  Remember this for later when
      // working out what proposal message to return.
      propose_first_unchosen = true;
    }

    activate({ .type = Value::Type::no_op },
      effective_slots.end() - first_inactive_slot);
  }

  split_active_slot_states_at(effective_slots.start());

  if (promise.type == Promise::Type::multi) {
    effective_slots.set_end(first_inactive_slot);
  } else {
    split_active_slot_states_at(effective_slots.end());
  }

  for (auto &a : active_slot_states) {
    if (!effective_slots.contains(a.slots.start())) { continue; }
    if (promise.term < a.term) { continue; }

    if (a.term < promise.term) {
      // abandon in favour of new proposal
      a.promises.clear();
      a.has_proposed_value = false;
      a.has_accepted_value = false;
      a.term               = promise.term;
    }

    if (!a.has_proposed_value) {
      a.promises.insert(acceptor);

      if (promise.type == Promise::Type::bound) {
        if (!a.has_accepted_value
            || a.max_accepted_term
                  < promise.max_accepted_term) {
          a.max_accepted_term       = promise.max_accepted_term;
          a.max_accepted_term_value = promise.max_accepted_term_value;
        }

        a.has_accepted_value = true;
      }
    }
  }

  if (promise.type == Promise::Type::multi) {

    if (current_term < promise.term) {
      // abandon
      promises_for_inactive_slots.clear();
      is_ready_to_propose = false;
      current_term        = promise.term;
    }

    if (current_term == promise.term
      && !is_ready_to_propose) {

      promises_for_inactive_slots.insert(acceptor);

      const auto &conf = configurations.find(current_term.era);
      if (conf != configurations.end()
          && conf->second.is_quorate(promises_for_inactive_slots)) {

        is_ready_to_propose = true;
        promises_for_inactive_slots.clear();
      }
    }
  }

  if (propose_first_unchosen) {
    const auto &a = *active_slot_states.begin();

    if (a.has_proposed_value) {
      // Value is still unbound - the promise doesn't apply to these slots.
      return {
        .slots = a.slots,
        .term  = a.term,
        .value = a.value
      };
    }
  }

  if (active_slot_states.empty()) {
    return empty_proposal;
  }

  auto &a = *active_slot_states.begin();
  if (a.slots.is_empty()) {
    return empty_proposal;
  }

  if (!a.has_proposed_value) {
    const auto &conf = configurations.find(a.term.era);
    if (conf != configurations.end()
          && conf->second.is_quorate(a.promises)) {
      a.promises.clear();
      a.has_proposed_value = true;
    }
  }

  if (a.has_proposed_value) {
    return {
      .slots = a.slots,
      .term  = a.term,
      .value = a.has_accepted_value
                      ? a.max_accepted_term_value
                      : a.value
    };
  } else {
    return empty_proposal;
  }
}

/* Splits active_slot_states so as to ensure that there is a boundary at
 * `slot`, as long as it is in [first_unchosen_slot,
 * first_inactive_slot] */
void Palladium::split_active_slot_states_at(const Slot slot) {
  if (slot == first_unchosen_slot) {
    return;
  }

  if (slot == first_inactive_slot) {
    return;
  }

  auto it = find_if(
          active_slot_states.begin(),
          active_slot_states.end(),
          [slot](const ActiveSlotState &a)
              { return a.slots.contains(slot); });

  if (slot == it[0].slots.start()) {
    return;
  }

  active_slot_states.insert(it, *it);

  // Iterator invalidated - find the right state again.
  it = find_if(
          active_slot_states.begin(),
          active_slot_states.end(),
          [slot](const ActiveSlotState &a)
              { return a.slots.contains(slot); });

  it[0].slots.set_end(slot);
  it[1].slots.truncate(slot);
}

const bool Palladium::search_for_quorums
        (std::vector<AcceptancesFromAcceptor>::const_iterator  pre_begin,
   const std::vector<AcceptancesFromAcceptor>::const_iterator &end,
         Proposal &chosen_message,
         Configuration::Weight accepted_weight,
   const Configuration::Weight total_weight) {

  if (total_weight < 2 * accepted_weight) {
    return true;
  }

  for (auto acceptor_iterator  = ++pre_begin;
            acceptor_iterator != end;
            acceptor_iterator++) {

    const auto &this_acceptor_weight = acceptor_iterator->weight;
    if (this_acceptor_weight == 0) { continue; }
    accepted_weight += this_acceptor_weight;

    for (auto &accepted_message : acceptor_iterator->proposals) {
      if (accepted_message.slots.start() != chosen_message.slots.start()) {
        continue;
      }

      if (accepted_message.slots.is_empty()) {
        continue;
      }

      if (accepted_message.term != chosen_message.term) {
        continue;
      }

      auto old_end = chosen_message.slots.end();
      if (accepted_message.slots.end() < chosen_message.slots.end()) {
        chosen_message.slots.set_end(accepted_message.slots.end());
      }

      if (search_for_quorums(acceptor_iterator, end, chosen_message, accepted_weight, total_weight)) {
        return true;
      }

      chosen_message.slots.set_end(old_end);
    }

    accepted_weight -= this_acceptor_weight;
  }

  return false;
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
  if (is_ready_to_propose) {
    o << "is_ready_to_propose = true" << std::endl;
  } else {
    o << "is_ready_to_propose = false" << std::endl;
    o << "promises_for_inactive_slots =";
    for (const auto &a : promises_for_inactive_slots) { o << ' ' << a; }
    o << std::endl;
  }
  o << "configuration       = v" << current_era
                             << ": " << current_configuration << std::endl;
  o << "configurations:" << std::endl;
  for (const auto &kvp : configurations) {
    o << "  v" << kvp.first << ": " << kvp.second << std::endl;
  }
  o << "received_acceptances:" << std::endl;
  for (const auto &from_acceptor : received_acceptances) {
    o << "  from " << from_acceptor.acceptor
            << "=" << (uint32_t)from_acceptor.weight << ":" << std::endl;
    for (const auto &msg : from_acceptor.proposals) {
      o << "    " << msg.term  << "@" << msg.slots << ": "
                  << msg.value << std::endl;
    }
  }
  o << "active_slot_states:" << std::endl;
  for (const auto &a : active_slot_states) {
    o << "  " << a.term << "@" << a.slots << ": " << a.value << std::endl;
    if (a.has_proposed_value) {
      o << "    - has_proposed_value" << std::endl;
    } else {
      o << "    - collecting promises:";
      for (const auto &n: a.promises) { o << ' ' << n; }
      o << std::endl;
    }
    if (a.has_accepted_value) {
      o << "    - bound to " << a.max_accepted_term
        << " = " << a.max_accepted_term_value << std::endl;
    } else {
      o << "    - free" << std::endl;
    }
  }

  return o;
}

}
