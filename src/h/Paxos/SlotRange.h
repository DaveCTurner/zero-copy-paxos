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



#ifndef PAXOS_SLOT_RANGE_H
#define PAXOS_SLOT_RANGE_H

#include "Paxos/basic_types.h"

#include <iostream>

namespace Paxos {

struct SlotRange {
  Slot _start; /* inclusive */
  Slot _end;   /* exclusive */

  SlotRange(const Slot &start, const Slot &end) {
    _start = start;
    _end   = end;
  }

  const Slot &start() const { return _start; }
  const Slot &end()   const { return _end;   }
  void set_end(const Slot &end) { _end = end; }

  const bool contains(const Slot &slot) const {
    return _start <= slot && slot < _end;
  }

  const bool is_empty() const {
    return _end <= _start;
  }

  const bool is_nonempty() const {
    return _start < _end;
  }

  const bool can_extend_with(const SlotRange &other) const {
    return _start <= other._end && other._start <= _end;
  }

  const void extend_with(const SlotRange &other) {
    if (other._start < _start) { _start = other._start; }
    if (_end     < other._end) { _end   = other._end;   }
  }

  const void truncate(const Slot &truncate_before) {
    if (_start < truncate_before) {
      _start = truncate_before;
    }
  }
};
std::ostream& operator<<(std::ostream&, const SlotRange&);

}

#endif // ndef PAXOS_SLOT_RANGE_H
