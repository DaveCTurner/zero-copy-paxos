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



#ifndef PAXOS_TERM_H
#define PAXOS_TERM_H

#include "Paxos/basic_types.h"
#include <iostream>

namespace Paxos {

struct Term {
  Era           era         = 0;
  TermNumber    term_number = 0;
  NodeId        owner = 0;

  Term() {}

  Term(Era e, TermNumber tn, NodeId p) {
    era         = e;
    term_number = tn;
    owner       = p;
  }
};

inline const bool operator==(const Term &t1, const Term &t2)
    __attribute__((always_inline));

inline const bool operator==(const Term &t1, const Term &t2) {
  return t1.era         == t2.era
      && t1.term_number == t2.term_number
      && t1.owner       == t2.owner;
}

inline const bool operator!=(const Term &t1, const Term &t2);

inline const bool operator!=(const Term &t1, const Term &t2) {
  return t1.era         != t2.era
      || t1.term_number != t2.term_number
      || t1.owner       != t2.owner;
}

inline const bool operator<(const Term &t1, const Term &t2)
    __attribute__((always_inline));

inline const bool operator<(const Term &t1, const Term &t2) {
#define LT_LEX(a,b,c) (((a) == (b)) ? (c) : (((a) < (b)) ? true : false))
  return LT_LEX(t1.era,          t2.era,
         LT_LEX(t1.term_number,  t2.term_number,
                t1.owner       < t2.owner));
#undef LT_LEX
}

std::ostream& operator<<(std::ostream&, const Term&);

}

#endif // ndef PAXOS_TERM_H
