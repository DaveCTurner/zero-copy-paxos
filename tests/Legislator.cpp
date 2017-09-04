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


#include "Paxos/Legislator.h"

using namespace Paxos;

Configuration create_conf();

class TracingOutsideWorld : public OutsideWorld {
  const instant start_time;
        instant current_time;

public:
  TracingOutsideWorld(instant current_time)
    : start_time(current_time),
      current_time(current_time)
    { }

  const instant get_current_time() override {
    return current_time;
  }
};

void legislator_test() {
  uint32_t seed = rand();
  std::cout << std::endl << "legislaor_test(): seed = " << seed << std::endl;
  srand(seed);

  auto conf = create_conf();
  TracingOutsideWorld world(std::chrono::steady_clock::now());
  Legislator legislator(world, 1, 0, 0, conf);

  std::cout << legislator << std::endl;
  legislator.handle_wake_up();

  std::cout << legislator << std::endl;
}
