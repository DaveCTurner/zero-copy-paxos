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



#include "Paxos/Palladium.h"

#include <chrono>

using namespace Paxos;
using namespace std::chrono;

Configuration create_conf();

void palladium_follower_speed_test() {
  auto conf = create_conf();
  Palladium pal(1, 0, 0, conf);

  auto t1 = high_resolution_clock::now();

  for (Slot i = 0; i < 10000; i++) {
    pal.handle_proposal({
        .slots = {
          .start =  i    * 1500,
          .end   = (i+1) * 1500
        },
        .term  = Term(0,0,2),
        .value = {.type = Value::Type::no_op}
      });

    pal.handle_accepted(1, {
          .slots = {
            .start =  i    * 1500,
            .end   = (i+1) * 1500
          },
          .term = Term(0,0,2),
          .value = {.type = Value::Type::no_op}
        });

    pal.check_for_chosen_slots();

    pal.handle_accepted(2, {
          .slots = {
            .start =  i    * 1500,
            .end   = (i+1) * 1500
          },
          .term = Term(0,0,2),
          .value = {.type = Value::Type::no_op}
        });

    pal.check_for_chosen_slots();
    pal.check_for_chosen_slots();
  }

  auto t2 = high_resolution_clock::now();

  std::cout << pal << std::endl << std::endl;

  duration<double> time_span = duration_cast<duration<double>>(t2 - t1);
  std::cout << "Duration: " << time_span.count() << "s" << std::endl;
}
