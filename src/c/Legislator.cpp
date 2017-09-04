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
  return o;
}

std::ostream& Paxos::operator<<(std::ostream &o, const Legislator &legislator) {
  return legislator.write_to(o);
}

std::chrono::steady_clock::duration Legislator::random_retry_delay() {
  return std::chrono::milliseconds(rand() % _retry_delay_ms);
}

void Legislator::handle_wake_up() {
  auto &now = _world.get_current_time();

  if (now < _next_wake_up) {
    return;
  }

  switch (_role) {
    case Role::candidate:
      // TODO
      set_next_wake_up_time(now + random_retry_delay());
      break;

    case Role::follower:
      _role = Role::candidate;
      // TODO
      set_next_wake_up_time(now + random_retry_delay());
      break;

    case Role::leader:
      _role = Role::incumbent;
      // TODO
      set_next_wake_up_time(now + _incumbent_timeout);
      break;

    case Role::incumbent:
      _role = Role::candidate;
      // TODO
      set_next_wake_up_time(now + random_retry_delay());
      break;
  }
}
