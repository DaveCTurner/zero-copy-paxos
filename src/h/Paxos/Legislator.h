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



#ifndef PAXOS_LEGISLATOR_H
#define PAXOS_LEGISLATOR_H

#include "Paxos/Palladium.h"
#include "Paxos/OutsideWorld.h"

namespace Paxos {

class Legislator {
  Legislator           (const Legislator&) = delete; // no copying
  Legislator &operator=(const Legislator&) = delete; // no assignment

  private:
    OutsideWorld &_world;
    Palladium     _palladium;

  public:
    Legislator( OutsideWorld&,
          const NodeId&,
          const Slot&,
          const Era&,
          const Configuration&);
};

}

#endif // ndef PAXOS_LEGISLATOR_H
