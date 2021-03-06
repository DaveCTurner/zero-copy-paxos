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
#include "Paxos/Proposal.h"

#include <algorithm>

using namespace Paxos;

Configuration create_conf() {
  Configuration conf(1);
  conf.increment_weight(1);
  conf.increment_weight(2);
  conf.increment_weight(2);
  conf.increment_weight(3);
  conf.increment_weight(3);
  return conf;
}

void assert_consistent(std::vector<Proposal> &chosens) {
  Slot i = 0;
  while (!chosens.empty()) {
    const auto first_matching = find_if(chosens.cbegin(),
                                        chosens.cend(),
      [i](const Proposal &p) { return p.slots.contains(i); });

    if (first_matching != chosens.cend()) {
      for (const auto &p : chosens) {
        if (p.slots.contains(i)) {
          assert(p.value == first_matching->value);
        }
      }
    }

    i++;

    chosens.erase(remove_if(chosens.begin(),
                            chosens.end(),
        [i](const Proposal &p) { return p.slots.end() <= i; }),
                            chosens.end());
  }
}
