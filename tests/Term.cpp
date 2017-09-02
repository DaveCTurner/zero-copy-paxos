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



#include "Paxos/Term.h"

#include <iostream>
#include <cassert>

using namespace Paxos;

void term_tests() {
  Term t1(1,2,3);

  std::cout << t1 << std::endl;

  assert(t1.era         == 1);
  assert(t1.term_number == 2);
  assert(t1.owner       == 3);
}
