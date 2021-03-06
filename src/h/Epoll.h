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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <chrono>
#include <atomic>
#include <thread>

using timestamp = std::chrono::time_point<std::chrono::steady_clock>;

namespace Epoll {

class ClockCache {
  public:
    virtual void set_current_time(const timestamp&) = 0;
};

class Handler {
  public:
    virtual void handle_readable () = 0;
    virtual void handle_writeable() = 0;
    virtual void handle_error    (const uint32_t) = 0;
};

void update_clock(std::atomic<bool>*, std::atomic<bool>*);

class Manager {
  Manager           (const Manager&) = delete; // no copying
  Manager &operator=(const Manager&) = delete; // no assignment

  const int epfd;
  ClockCache &clock_cache;

  std::atomic<bool> clock_updater_should_exit;
  std::atomic<bool> clock_needs_updating;
  std::thread       clock_updater;

  void ctl_and_verify(int op,
                      int fd,
                      Handler *handler,
                      uint32_t events) {
    struct epoll_event event;
    event.events = events;
    event.data.ptr = static_cast<void*>(handler);
    if (epoll_ctl(epfd, op, fd, &event) == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: epoll_ctl(%d, %d, %d, %u/%p) failed\n",
        __PRETTY_FUNCTION__, epfd, op, fd, event.events, event.data.ptr);
      abort();
    }
  }

  public:
  Manager(ClockCache &clock_cache)
      : epfd(epoll_create(1)),
        clock_cache(clock_cache),
        clock_updater_should_exit(false),
        clock_needs_updating(true),
        clock_updater(update_clock, &clock_needs_updating,
                                    &clock_updater_should_exit) {

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
    clock_updater_should_exit.store(true);
    clock_updater.join();
  }

  void register_handler(int fd, Handler *handler, uint32_t events) {
    ctl_and_verify(EPOLL_CTL_ADD, fd, handler, events);
  }

  void modify_handler(int fd, Handler *handler, uint32_t events) {
    ctl_and_verify(EPOLL_CTL_MOD, fd, handler, events);
  }

  void deregister_handler(int fd) {
    ctl_and_verify(EPOLL_CTL_DEL, fd, NULL, 0);
  }

  void deregister_close_and_clear(int &fd) {
    if (fd != -1) {
      deregister_handler(fd);
      close(fd);
      fd = -1;
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

    if (clock_needs_updating.load()) {
      clock_needs_updating.store(false);
      clock_cache.set_current_time(std::chrono::steady_clock::now());
    }

#ifndef NTRACE
    printf("%s: %d events received\n", __PRETTY_FUNCTION__, event_count);
#endif // ndef NTRACE

    for (int i = 0; i < event_count; i++) {
      auto &e = events[i];
      auto event_bits = e.events;
      auto handler = static_cast<Handler*>(e.data.ptr);
      assert(handler != NULL);

      if (event_bits & ~(EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP)) {
        handler->handle_error(event_bits);
      } else {
        if (event_bits & (EPOLLIN | EPOLLHUP | EPOLLRDHUP)) {
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
