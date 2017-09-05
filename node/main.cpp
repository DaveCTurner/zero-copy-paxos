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

#include "RealWorld.h"
#include "Paxos/Legislator.h"

int main(int argc, char **argv) {
  Paxos::Configuration conf(1);
  RealWorld real_world("3277f758-473b-4188-99fc-b19b0e7a940b", 1);
  Paxos::Legislator legislator(real_world, 1, 0, 0, conf);

  return 0;
}
