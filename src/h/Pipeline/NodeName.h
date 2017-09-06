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



#ifndef PIPELINE_NODE_NAME_H
#define PIPELINE_NODE_NAME_H

#include "Paxos/basic_types.h"
#include <string>

namespace Pipeline {

struct NodeName {
  const std::string   &cluster;
  const Paxos::NodeId  id;

  NodeName(const std::string &cluster, const Paxos::NodeId id)
    : cluster(cluster), id(id) {}

  NodeName           (const NodeName&) = delete;
  NodeName &operator=(const NodeName&) = delete;
};

}

#endif // ndef PIPELINE_NODE_NAME_H
