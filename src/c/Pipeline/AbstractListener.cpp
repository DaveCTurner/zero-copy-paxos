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



#include "Pipeline/AbstractListener.h"
#include "Epoll.h"

#include <sys/socket.h>
#include <netdb.h>
#include <memory>
#include <string.h>

namespace Pipeline {

int AbstractListener::tcp_open_and_listen(const char *port) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags    = AI_PASSIVE;

  struct addrinfo *ai;
  int getaddrinfo_result = getaddrinfo(NULL, port, &hints, &ai);
  if (getaddrinfo_result != 0) {
    fprintf(stderr, "%s: getaddrinfo() failed: %s\n",
      __PRETTY_FUNCTION__,
      gai_strerror(getaddrinfo_result));
    return -1;
  }

  for (struct addrinfo *p = ai; p != NULL; p = p->ai_next) {
    int fd = socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK, p->ai_protocol);
    if (fd == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: socket() failed\n", __PRETTY_FUNCTION__);
      continue;
    }

    int enable = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: setsockopt(SO_REUSEADDR) failed\n",
        __PRETTY_FUNCTION__);
      continue;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(int)) == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: setsockopt(SO_REUSEPORT) failed\n",
        __PRETTY_FUNCTION__);
      continue;
    }

    if (bind(fd, p->ai_addr, p->ai_addrlen) == -1) {
      close(fd);
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: bind() failed\n", __PRETTY_FUNCTION__);
      continue;
    }

    if (listen(fd, 10) == -1) {
      close(fd);
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: listen() failed\n", __PRETTY_FUNCTION__);
      continue;
    }

    freeaddrinfo(ai);
    return fd;
  }

  freeaddrinfo(ai);
  return -1;
}

AbstractListener::AbstractListener(Epoll::Manager &manager,
                                   const char *port)
    : manager(manager) {

  assert(port != NULL);
  fd = tcp_open_and_listen(port);

  if (fd == -1) {
    fprintf(stderr, "%s: tcp_open_and_listen(%s) failed\n",
      __PRETTY_FUNCTION__, port);
    abort();
  }

  manager.register_handler(fd, this, EPOLLIN);

#ifndef NTRACE
  printf("%s: fd=%d\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
}

AbstractListener::~AbstractListener() {
#ifndef NTRACE
  printf("%s: fd=%d\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
  manager.deregister_close_and_clear(fd);
}

void AbstractListener::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  abort();
}

void AbstractListener::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  abort();
}

void AbstractListener::handle_readable() {
  int client_fd = accept4(fd, NULL, NULL, SOCK_NONBLOCK);
  if (client_fd == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: accept4() failed\n", __PRETTY_FUNCTION__);
    abort();
  }

#ifndef NTRACE
  printf("%s: fd=%d, client_fd=%d\n",
           __PRETTY_FUNCTION__,
           fd, client_fd);
#endif // ndef NTRACE

  handle_accept(client_fd);
}

}
