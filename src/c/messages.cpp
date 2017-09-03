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



#include <iostream>

#include "Paxos/Promise.h"
#include "Paxos/Proposal.h"

namespace Paxos {

std::ostream& operator<<(std::ostream &o, const Proposal &p) {
  return o << p.slots
   << ": " << p.term
   << ": " << p.value;
}

std::ostream& operator<<(std::ostream &o, const Promise &p) {
  switch (p.type) {
    case Promise::Type::none:
      return o << "[no promise]";
    case Promise::Type::multi:
      return o << p.term << ":[" << p.slots.start() << ",oo)";
    case Promise::Type::free:
      return o << p.term << ":" << p.slots;
    case Promise::Type::bound:
      return o << p.term << ":" << p.slots
        << "=" << p.max_accepted_term
          << ":" << p.max_accepted_term_value;
  }
  return o;
}

}
