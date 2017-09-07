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



#ifndef PIPELINE_CLIENT_CHOSEN_STREAM_CONTENT_HANDLER_H
#define PIPELINE_CLIENT_CHOSEN_STREAM_CONTENT_HANDLER_H

#include "Paxos/Proposal.h"

namespace Pipeline {
namespace Client {

class ChosenStreamContentHandler {
public:
  virtual void handle_stream_content(const Paxos::Proposal&) = 0;
  virtual void handle_non_contiguous_stream_content(const Paxos::Proposal&) = 0;
  virtual void handle_unknown_stream_content(const Paxos::Proposal&) = 0;
};

}
}

#endif // ndef PIPELINE_CLIENT_CHOSEN_STREAM_CONTENT_HANDLER_H
