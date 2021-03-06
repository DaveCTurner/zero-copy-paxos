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



#ifndef PAXOS_BASIC_TYPES_H
#define PAXOS_BASIC_TYPES_H

#include <stdint.h>

namespace Paxos {

typedef uint64_t Slot;
typedef uint32_t Era;
typedef uint32_t TermNumber;
typedef uint32_t NodeId;

}

#define   LIKELY(condition) __builtin_expect(static_cast<bool>(condition), 1)
#define UNLIKELY(condition) __builtin_expect(static_cast<bool>(condition), 0)

#endif // ndef PAXOS_BASIC_TYPES_H
