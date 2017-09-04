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

using delay   = std::chrono::steady_clock::duration;

namespace Paxos {

class Legislator {
  Legislator           (const Legislator&) = delete; // no copying
  Legislator &operator=(const Legislator&) = delete; // no assignment

  public:
    enum Role {
      candidate,
      follower,
      leader,
      incumbent
    };

  private:
    OutsideWorld &_world;
    Palladium     _palladium;

    instant   _next_wake_up      = _world.get_current_time();
    Role      _role              = Role::candidate;
    delay     _incumbent_timeout = std::chrono::milliseconds(100);
    int       _retry_delay_ms    = 1000;

    void set_next_wake_up_time(const instant &t) {
      _next_wake_up = t;
      _world.set_next_wake_up_time(_next_wake_up);
    }

    std::chrono::steady_clock::duration random_retry_delay();

  public:
    Legislator( OutsideWorld&,
          const NodeId&,
          const Slot&,
          const Era&,
          const Configuration&);

    std::ostream &write_to(std::ostream&) const;

    void handle_wake_up();

};
std::ostream& operator<<(std::ostream&, const Legislator&);

}

#endif // ndef PAXOS_LEGISLATOR_H
