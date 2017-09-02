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



#include "Paxos/Configuration.h"

#include <algorithm>
#include <cassert>
#include <string.h>
#include <unistd.h>

namespace Paxos {

const bool Configuration::is_quorate(const std::set<NodeId> &acceptors) const {
  uint8_t total_weight    = 0;
  uint8_t accepted_weight = 0;

  for (const auto &entry : entries) {
    assert(entry.weight() > 0);
    total_weight += entry.weight();
    if (acceptors.find(entry.node_id()) != acceptors.end()) {
      accepted_weight += entry.weight();
    }
  }

  return 0 < total_weight
          && total_weight < 2 * accepted_weight;
}

std::vector<Configuration::Entry>::iterator
  Configuration::find(const NodeId &aid) {

  return find_if(entries.begin(),
                 entries.end(),
                 [aid](const Configuration::Entry &e)
                 { return e.node_id() == aid; });
}

void Configuration::increment_weight(const NodeId &aid){
  if (total_weight() == 255) { return; }
  auto it = find(aid);
  if (it == entries.end()) {
    entries.push_back(Configuration::Entry(aid, 1));
  } else {
    assert(it->weight() > 0);
    it->inc_weight();
  }
}

void Configuration::decrement_weight(const NodeId &aid){
  if (total_weight() <= 1) { return; }
  auto it = find(aid);
  if (it == entries.end()) { return; }
  assert(it->weight() > 0);
  if (it->weight() == 1) {
    entries.erase(it);
  } else {
    it->dec_weight();
  }
}

std::ostream& operator<<(std::ostream &o, const Configuration::Entry &entry) {
  return o << entry.node_id()
    << "=" << (uint32_t) entry.weight();
}

std::ostream& operator<<(std::ostream &o, const Configuration &conf) {
  bool first = true;
  for (const auto &entry : conf.entries) {
    if (first) {
      first = false;
    } else {
      o << ';';
    }
    o << entry;
  }
  return o;
}

}
