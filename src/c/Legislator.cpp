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

void Legislator::handle_wake_up() {
  // TODO
}
