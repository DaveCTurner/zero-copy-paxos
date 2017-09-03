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



#ifndef PAXOS_PROMISE_H
#define PAXOS_PROMISE_H

#include "Paxos/Term.h"
#include "Paxos/SlotRange.h"
#include "Paxos/Value.h"

namespace Paxos {

struct Promise {
  enum Type : uint8_t {
    none = 0,
    multi,
    free,
    bound
  };

  Type      type;

  SlotRange slots;
  /* slots.end relevant iff type is bound or free */

  Term      term;

  /* relevant iff type is bound */
  Term      max_accepted_term;
  Value     max_accepted_term_value;

  Promise(const Type &type,
          const Slot &start,
          const Slot &end,
          const Term &term)
    : type(type), slots(start,end), term(term) {}
};
std::ostream& operator<<(std::ostream&, const Promise&);

}

#endif // ndef PAXOS_PROMISE_H
