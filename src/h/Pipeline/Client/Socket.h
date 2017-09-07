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



#ifndef PIPELINE_CLIENT_SOCKET_H
#define PIPELINE_CLIENT_SOCKET_H

#include "Pipeline/Pipe.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"

namespace Pipeline {
namespace Client {

class Socket : public Epoll::Handler {
  Socket           (const Socket&) = delete; // no copying
  Socket &operator=(const Socket&) = delete; // no assignment

private:
        Epoll::Manager            &manager;
        Paxos::Legislator         &legislator;

  const NodeName                  &node_name;
  const Paxos::Value::StreamName   stream;

        Pipe<Socket>               pipe;
        int                        fd;

  void shutdown();

public:
  Socket (Epoll::Manager&,
          Paxos::Legislator&,
          const NodeName&,
          const Paxos::Value::StreamName,
          const int);

  ~Socket();

  bool is_shutdown() const;

  void handle_readable() override;
  void handle_writeable() override;
  void handle_error(const uint32_t) override;

  bool ok_to_write_data() const;
  const Paxos::Term &get_term_for_next_write() const;
  const Paxos::Value::StreamOffset get_offset_for_next_write(uint64_t) const;

  void downstream_became_writeable();
  void downstream_wrote_bytes(uint64_t, uint64_t);
};

}
}

#endif // ndef PIPELINE_CLIENT_SOCKET_H
