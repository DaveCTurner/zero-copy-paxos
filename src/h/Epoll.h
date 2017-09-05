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

class Handler {
  public:
    virtual void handle_readable () = 0;
    virtual void handle_writeable() = 0;
    virtual void handle_error    (const uint32_t) = 0;
};

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

#ifndef NTRACE
    printf("%s: epfd=%d\n", __PRETTY_FUNCTION__, epfd);
#endif // ndef NTRACE
  }

  ~Manager() {
#ifndef NTRACE
    printf("%s: epfd=%d\n", __PRETTY_FUNCTION__, epfd);
#endif // ndef NTRACE

    if (epfd != -1) {
      close(epfd);
    }
  }

  void wait(int timeout_milliseconds) {
#ifndef NTRACE
    printf("\n%s: timeout=%d\n", __PRETTY_FUNCTION__, timeout_milliseconds);
#endif // ndef NTRACE

#define EPOLL_EVENTS_SIZE 20
    struct epoll_event events[EPOLL_EVENTS_SIZE];
    int event_count = epoll_wait(epfd,
                                 events,
                                 EPOLL_EVENTS_SIZE,
                                 timeout_milliseconds);
#undef EPOLL_EVENTS_SIZE

#ifndef NTRACE
    printf("%s: %d events received\n", __PRETTY_FUNCTION__, event_count);
#endif // ndef NTRACE

    for (int i = 0; i < event_count; i++) {
      auto &e = events[i];
      auto event_bits = e.events;
      auto handler = static_cast<Handler*>(e.data.ptr);
      assert(handler != NULL);

      if (event_bits & ~(EPOLLIN | EPOLLOUT)) {
        handler->handle_error(event_bits);
      } else {
        if (event_bits & (EPOLLIN)) {
          handler->handle_readable();
        }
        if (event_bits & EPOLLOUT) {
          handler->handle_writeable();
        }
      }
    }
  }
};

}

#endif // ndef EPOLL_H
