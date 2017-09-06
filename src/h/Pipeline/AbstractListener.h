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



#ifndef PIPELINE_ABSTRACT_LISTENER_H
#define PIPELINE_ABSTRACT_LISTENER_H

#include "Epoll.h"

#include <sys/socket.h>
#include <netdb.h>
#include <memory>
#include <string.h>

namespace Pipeline {

class AbstractListener : public Epoll::Handler {
  AbstractListener           (const AbstractListener&) = delete; // no copying
  AbstractListener &operator=(const AbstractListener&) = delete; // no assignment

protected:
  Epoll::Manager        &manager;
  virtual void handle_accept(int) = 0;

private:
  int                    fd;
  static int tcp_open_and_listen(const char*);

  public:
  AbstractListener(Epoll::Manager&, const char*);
  ~AbstractListener();

  void handle_writeable()           override;
  void handle_error(const uint32_t) override;
  void handle_readable()            override;
};

}

#endif // ndef PIPELINE_ABSTRACT_LISTENER_H
