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

#include "Paxos/Term.h"
#include "Paxos/Configuration.h"

#include <iostream>

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
  NodeId _node_id;
  Slot   first_unchosen_slot;

  /* Proposer *****************************************************/
  Slot first_inactive_slot;
  Term current_term;

  /* Learner ******************************************************/
  /* Configuration of the first unchosen slot. */
  Era           current_era;
  Configuration current_configuration;

public:
  Palladium(const NodeId, const Slot,
            const Era, const Configuration&);

  const NodeId &node_id() const { return _node_id; }

  std::ostream& write_to(std::ostream &) const;
};
std::ostream& operator<<(std::ostream&, const Palladium&);

}

#endif // ndef PAXOS_PALLADIUM_H
