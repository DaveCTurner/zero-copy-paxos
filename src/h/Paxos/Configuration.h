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



#ifndef PAXOS_CONFIGURATION_H
#define PAXOS_CONFIGURATION_H

#include "Paxos/basic_types.h"

#include <iostream>
#include <vector>

namespace Paxos {

struct Configuration {
  typedef uint8_t     Weight;

  struct Entry {
  private:
    NodeId      _node_id;
    Weight      _weight;

  public:
    Entry(NodeId n, Weight w)
      : _node_id(n),
        _weight(w) {}

    const NodeId &node_id() const { return _node_id; }
    const Weight &weight()  const { return _weight; }
  };

  std::vector<Entry> entries;

  Configuration(const NodeId &acceptor) {
    entries.push_back(Entry(acceptor, 1));
  }

  Configuration(const std::vector<Entry> &entries)
    : entries(entries) { }
};
std::ostream& operator<<(std::ostream&, const Configuration&);
std::ostream& operator<<(std::ostream&, const Configuration::Entry&);

}

#endif // ndef PAXOS_CONFIGURATION_H
