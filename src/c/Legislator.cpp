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
  o << _palladium;
  if (_seeking_votes) {
    o << "offered_votes           =";
    for (auto const &n : _offered_votes) {
      o << " " << n;
    }
    o << std::endl;
  } else {
    o << "offered_votes           = not_seeking" << std::endl;
  }
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

      handle_offer_vote(_palladium.node_id());

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
      // TODO propose a no-op value
      set_next_wake_up_time(now + _incumbent_timeout);
      break;
  }
}

void Legislator::handle_offer_vote(const NodeId &peer_id) {
  if (_seeking_votes) {
    _offered_votes.insert(peer_id);
    if (_palladium.get_current_configuration().is_quorate(_offered_votes)) {
      _seeking_votes = false;
      _offered_votes.clear();
      // TODO achieved a quorum of offers - start a new term
    }
  }
}
