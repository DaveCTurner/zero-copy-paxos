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
        instant next_wake_up_time;

public:
  TracingOutsideWorld(instant current_time)
    : start_time(current_time),
      current_time(current_time),
      next_wake_up_time(current_time)
    { }

  const instant get_current_time() override {
    return current_time;
  }

  void set_next_wake_up_time(const instant &t) override {
    next_wake_up_time = t;
    std::cout << "set_next_wake_up_time(" <<
      std::chrono::duration_cast<std::chrono::milliseconds>
        (t-start_time).count()
      << "ms)" << std::endl;
  }

  void tick() {
    current_time = next_wake_up_time;
    std::cout << "current_time(" <<
      std::chrono::duration_cast<std::chrono::milliseconds>
        (current_time-start_time).count()
      << "ms)" << std::endl;
  }

  void seek_votes_or_catch_up(const Slot &slot) override {
    std::cout << "seek_votes_or_catch_up("
      << slot << ")" << std::endl;
  }

  void request_catch_up(const NodeId &recipient) override {
    std::cout << "request_catch_up("
      << recipient << ")" << std::endl;
  }
};

void legislator_test() {
  uint32_t seed = rand();
  std::cout << std::endl << "legislaor_test(): seed = " << seed << std::endl;
  srand(seed);

  auto conf = create_conf();
  TracingOutsideWorld world(std::chrono::steady_clock::now());
  Legislator legislator(world, 1, 0, 0, conf);

  std::cout << std::endl << "TEST: Initial state" << std::endl;
  std::cout << legislator << std::endl;
  world.tick();

  std::cout << std::endl << "TEST: handle_wake_up()" << std::endl;
  legislator.handle_wake_up();
  std::cout << legislator << std::endl;

  std::cout << std::endl << "TEST: handle_offer_catch_up(3)" << std::endl;
  legislator.handle_offer_catch_up(3);
  std::cout << legislator << std::endl;

  world.tick();
  std::cout << std::endl << "TEST: handle_wake_up()" << std::endl;
  legislator.handle_wake_up();
  std::cout << legislator << std::endl;

  std::cout << std::endl << "TEST: handle_offer_vote(2)" << std::endl;
  legislator.handle_offer_vote(2);
  std::cout << legislator << std::endl;
}
