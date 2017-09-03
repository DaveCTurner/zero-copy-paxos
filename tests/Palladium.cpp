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

using namespace Paxos;

Configuration create_conf();

void palladium_tests() {
  auto conf = create_conf();

  Palladium pal(1, 0, 0, conf);
  std::cout << "Initial state: " << std::endl << pal << std::endl << std::endl;

  std::cout << "ACTIVATE(NO-OP x3) emitted:" << pal.activate({.type = Value::Type::no_op}, 3) << std::endl;
  std::cout << pal << std::endl << std::endl;

  std::cout << "PREP[0.0.2] emitted: " << pal.handle_prepare(Term(0,0,2)) << std::endl << std::endl;
  std::cout << pal << std::endl << std::endl;

  std::cout << "PROM[0.0.1]:[0,2)@1 emitted: " << pal.handle_promise(1,
    Promise(Promise::Type::free, 0, 2, Term(0,0,1))) << std::endl << std::endl;
  std::cout << pal << std::endl << std::endl;

  std::cout << "PROM[0.0.1]:[1,oo)@2 emitted: " << pal.handle_promise(2,
    Promise(Promise::Type::multi, 1, 1, Term(0,0,1))) << std::endl << std::endl;
  std::cout << pal << std::endl << std::endl;

  std::cout << "PROM[0.0.1]:[0,oo)@3 emitted: " << pal.handle_promise(3,
    Promise(Promise::Type::multi, 0, 0, Term(0,0,1))) << std::endl << std::endl;
  std::cout << pal << std::endl << std::endl;
}

