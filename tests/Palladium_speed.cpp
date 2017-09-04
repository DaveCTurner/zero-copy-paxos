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

void palladium_leader_speed_test() {

  auto conf = create_conf();

  Palladium pal(1, 0, 0, conf);

  pal.handle_promise(1,
      Promise(Promise::Type::multi, 0, 0, Term(0,0,1)));

  pal.handle_promise(2,
      Promise(Promise::Type::multi, 0, 0, Term(0,0,1)));

  std::cout << "Initial state: " << std::endl << pal << std::endl << std::endl;

  auto t1 = high_resolution_clock::now();
  for (Slot i = 0; i < 10000; i++) {
    Value value = { .type = Value::Type::stream_content };
    value.payload.stream.name.owner = 1;
    value.payload.stream.name.id    = 2;
    value.payload.stream.offset     = 0;

    const Slot first_activated __attribute__((unused))
                          = i * 1500;
    const auto activate_result = pal.activate(value, 1500);

    pal.handle_proposal(activate_result);
    pal.handle_accepted(1, activate_result);
    pal.check_for_chosen_slots();

    if (i > 10) {
      const NodeId peer = (i%2==0) ? 2 : 3;

      pal.handle_accepted(peer, {
        .slots = {
          .start = 0,
          .end   = (i - 10) * 1500
        },
        .term = pal.next_activated_term(),
        .value = value
      });
      pal.check_for_chosen_slots();
    }

    pal.check_for_chosen_slots();
  }

  auto t2 = high_resolution_clock::now();

  std::cout << pal << std::endl << std::endl;

  duration<double> time_span = duration_cast<duration<double>>(t2 - t1);
  std::cout << "Duration: " << time_span.count() << "s" << std::endl;
}

