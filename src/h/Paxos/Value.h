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



#ifndef PAXOS_VALUE_H
#define PAXOS_VALUE_H

#include "Paxos/Configuration.h"

namespace Paxos {

struct Value {
  enum Type : uint8_t
  {
    no_op = 0,
    generate_node_id,
    reconfiguration_inc,
    reconfiguration_dec,
    reconfiguration_mul,
    reconfiguration_div,
    stream_content
  };

  typedef uint32_t StreamId;
  typedef uint64_t StreamOffset;

  struct StreamName {
    NodeId owner;
    StreamId id;
  };

  struct OffsetStream {
    StreamName   name;
    StreamOffset offset;
  };

  union Reconfiguration {
    NodeId                   subject; // For _inc and _dec: the affected node
    Configuration::Weight    factor;  // For _mul and _div: the multiplier or divisor
  };

  union Payload {
    /* Applies to Type::reconfiguration_*.  Depending on .type, only
     * some of the fields here are in use. */
    Reconfiguration reconfiguration;

    /* Applies to Type::generate_node_id. */
    NodeId originator;

    /* Applies to Type::stream_content */
    OffsetStream stream;
  };

  Type type;
  Payload payload;
};

std::ostream& operator<<(std::ostream&, const Value&);
std::ostream& operator<<(std::ostream&, const Value::StreamName&);
std::ostream& operator<<(std::ostream&, const Value::OffsetStream&);

inline const bool operator==(const Value&, const Value&)
    __attribute__((always_inline));

inline const bool operator==(const Value &v1, const Value &v2) {
  if (v1.type != v2.type) { return false; }

  if (v1.type == Value::Type::no_op) { return true; }
  if (v1.type == Value::Type::generate_node_id)
    { return v1.payload.originator == v2.payload.originator; }

  if (v1.type == Value::Type::stream_content) {
    return  v1.payload.stream.name.owner
         == v2.payload.stream.name.owner
      &&    v1.payload.stream.name.id
         == v2.payload.stream.name.id
      &&    v1.payload.stream.offset
         == v2.payload.stream.offset;
  }

  const auto &a1 = v1.payload.reconfiguration;
  const auto &a2 = v2.payload.reconfiguration;

  switch (v1.type) {
    case Value::Type::reconfiguration_inc:
    case Value::Type::reconfiguration_dec:
      return a1.subject == a2.subject;

    case Value::Type::reconfiguration_mul:
    case Value::Type::reconfiguration_div:
      return a1.factor == a2.factor;

    default:
      assert(false); // Other cases already handled
      return false;
  }
}

}

#endif // ndef PAXOS_VALUE_H
