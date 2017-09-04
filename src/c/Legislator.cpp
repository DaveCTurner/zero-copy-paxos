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



#include "Paxos/Legislator.h"

using namespace Paxos;

Legislator::Legislator(OutsideWorld  &world,
                 const NodeId        &node_id,
                 const Slot          &initial_slot,
                 const Era           &initial_era,
                 const Configuration &initial_conf)
  : _world(world),
    _palladium(node_id, initial_slot, initial_era, initial_conf) {
}

std::ostream &Legislator::write_to(std::ostream &o) const {
  o << "-- palladium" << std::endl;
  o << _palladium;
  o << "-- timeout & roles:" << std::endl;
  o << "next_wake_up            = " <<
    std::chrono::time_point_cast<std::chrono::milliseconds>
      (_next_wake_up).time_since_epoch().count()
    << "ms" << std::endl;
  o << "retry_delay_ms          = " << _retry_delay_ms << "ms" << std::endl;

  o << "role                    = " << _role << " (";
  switch (_role) {
    case Role::candidate:
      o << "candidate";
      break;
    case Role::follower:
      o << "follower";
      break;
    case Role::leader:
      o << "leader";
      break;
    case Role::incumbent:
      o << "incumbent";
      break;
    default:
      o << "??";
      break;
  }
  o << ")" << std::endl;
  o << "leader                  = " << _leader_id << std::endl;

  o << "-- re-election:" << std::endl;
  if (_seeking_votes) {
    o << "offered_votes           =";
    for (auto const &n : _offered_votes) {
      o << " " << n;
    }
    o << std::endl;
  } else {
    o << "offered_votes           = not_seeking" << std::endl;
  }
  o << "minimum_term_for_peers  = " << _minimum_term_for_peers << std::endl;
  o << "attempted_term          = " << _attempted_term         << std::endl;
  return o;
}

std::ostream& Paxos::operator<<(std::ostream &o, const Legislator &legislator) {
  return legislator.write_to(o);
}

std::chrono::steady_clock::duration Legislator::random_retry_delay() {
  if (_retry_delay_ms <= _minimum_retry_delay_ms) {
    return std::chrono::milliseconds(_minimum_retry_delay_ms);
  } else {
    return std::chrono::milliseconds(
         _minimum_retry_delay_ms + rand() % (_retry_delay_ms - _minimum_retry_delay_ms));
  }
}

void Legislator::handle_wake_up() {
  auto &now = _world.get_current_time();

  if (now < _next_wake_up) {
    return;
  }

  switch (_role) {
    case Role::candidate:
      _retry_delay_ms += _retry_delay_increment_ms;
      if (_retry_delay_ms > _maximum_retry_delay_ms) {
        _retry_delay_ms = _maximum_retry_delay_ms;
      }

      _offered_votes.clear();
      _seeking_votes = true;
      _world.seek_votes_or_catch_up(_palladium.next_chosen_slot());

      handle_offer_vote(_palladium.node_id(),
                        _palladium.get_min_acceptable_term());

      set_next_wake_up_time(now + random_retry_delay());
      break;

    case Role::follower:
    case Role::incumbent:
      std::cout << "Leadership timed out, becoming candidate" << std::endl;
      _role = Role::candidate;
      _retry_delay_ms = _minimum_retry_delay_ms;
      set_next_wake_up_time(now + random_retry_delay());
      break;

    case Role::leader:
      _role = Role::incumbent;
      activate_slots(Value{.type = Value::Type::no_op}, 1);
      set_next_wake_up_time(now + _incumbent_timeout);
      break;
  }
}

void Legislator::handle_seek_votes_or_catch_up
      (const NodeId      &peer_id,
       const Slot        &slot) {

  if (slot < _palladium.next_chosen_slot()) {
    // TODO offer catch-up
  } else if (slot == _palladium.next_chosen_slot()) {
    _world.offer_vote(peer_id, _palladium.get_min_acceptable_term());
  }
}

void Legislator::handle_offer_vote(const NodeId &peer_id,
                                   const Term   &min_acceptable_term) {
  if (_minimum_term_for_peers < min_acceptable_term) {
    _minimum_term_for_peers = min_acceptable_term;
  }

  if (_seeking_votes) {
    _offered_votes.insert(peer_id);
    if (_palladium.get_current_configuration().is_quorate(_offered_votes)) {
      _seeking_votes = false;
      _offered_votes.clear();
      start_term(_palladium.node_id());
    }
  }
}

void Legislator::handle_offer_catch_up(const NodeId &sender) {
  if (_seeking_votes) {
    _seeking_votes = false;
    _offered_votes.clear();
    _world.request_catch_up(sender);
  }
}

void Legislator::abdicate_to(const NodeId &node_id) {
  start_term(node_id);
}

void Legislator::start_term(const NodeId &owner_id) {
  if (_attempted_term < _minimum_term_for_peers) {
    _attempted_term = _minimum_term_for_peers;
  }

  const Term &minimum_term_for_self = _palladium.get_min_acceptable_term();
  if (_attempted_term < minimum_term_for_self) {
    _attempted_term = minimum_term_for_self;
  }

  if (owner_id < _attempted_term.owner) {
    _attempted_term.term_number += 1;
  }
  _attempted_term.owner = owner_id;

  _world.prepare_term(_attempted_term);
  handle_prepare_term(_palladium.node_id(), _attempted_term);
}

void Legislator::handle_prepare_term(const NodeId &sender, const Term &term) {
  auto promise = _palladium.handle_prepare(term);
  if (promise.type == Promise::Type::multi
      || promise.slots.is_nonempty()) {
    if (term.owner == _palladium.node_id()) {
      handle_promise(_palladium.node_id(), promise);
    } else {
      _world.make_promise(promise);
    }
  }
}

void Legislator::handle_promise(const NodeId &sender, const Promise &promise) {
  handle_proposal(_palladium.handle_promise(sender, promise));

  if (!_palladium.has_active_slots()) {
    handle_proposal(_palladium.activate({.type = Value::Type::no_op}, 1));
  }
}
