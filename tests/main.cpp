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


#include <iostream>

void term_tests();
void slot_range_tests();
void palladium_tests();
void palladium_random_safety_test();
void palladium_follower_speed_test();
void palladium_leader_speed_test();
void legislator_test();

int main() {
  srand(time(NULL));

  term_tests();
  slot_range_tests();
  palladium_tests();
  for (int i = 0; i < 1; i++) {
    palladium_random_safety_test();
  }
  palladium_follower_speed_test();
  palladium_leader_speed_test();

  legislator_test();

  std::cout << std::endl << "ALL OK" << std::endl << std::endl;
  return 0;
}

