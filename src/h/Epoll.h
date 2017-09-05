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



#ifndef EPOLL_H
#define EPOLL_H

#include <sys/epoll.h>
#include <unistd.h>

namespace Epoll {

class Manager {
  Manager           (const Manager&) = delete; // no copying
  Manager &operator=(const Manager&) = delete; // no assignment

  const int epfd;

  public:
  Manager()
      : epfd(epoll_create(1)) {
    if (epfd == -1) {
      perror(__PRETTY_FUNCTION__);
      abort();
    }
  }

  ~Manager() {
    if (epfd != -1) {
      close(epfd);
    }
  }

  void wait(int timeout_milliseconds) {
#define EPOLL_EVENTS_SIZE 20
    struct epoll_event events[EPOLL_EVENTS_SIZE];
    epoll_wait(epfd,
               events,
               EPOLL_EVENTS_SIZE,
               timeout_milliseconds);
#undef EPOLL_EVENTS_SIZE
  }
};

}

#endif // ndef EPOLL_H
