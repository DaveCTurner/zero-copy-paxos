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



#include "Paxos/Value.h"
#include <cassert>

namespace Paxos {

std::ostream& operator<<(std::ostream &o, const Value &value) {
  switch (value.type) {
    case Value::Type::no_op:
      return o << "NO-OP";
    case Value::Type::generate_node_id:
      return o << "GEN-ID(" << value.payload.originator << ")";
    case Value::Type::reconfiguration_inc:
      return o << "INC(" << value.payload.reconfiguration.subject << ")";
    case Value::Type::reconfiguration_dec:
      return o << "DEC(" << value.payload.reconfiguration.subject << ")";
    case Value::Type::reconfiguration_mul:
      return o << "MUL(" << (uint32_t)value.payload.reconfiguration.factor << ")";
    case Value::Type::reconfiguration_div:
      return o << "DIV(" << (uint32_t)value.payload.reconfiguration.factor << ")";
    case Value::Type::stream_content:
      return o << "STREAM(" << value.payload.stream << ")";
    default:
      return o << "UNKNOWN(" << value.type << ")";
  }
  return o;
}

std::ostream& operator<<(std::ostream &o, const Value::StreamName &sn) {
  return o << "s" << sn.owner << "." << sn.id;
}

std::ostream& operator<<(std::ostream &o, const Value::OffsetStream &os) {
  return o << os.name << "@" << os.offset;
}

}
