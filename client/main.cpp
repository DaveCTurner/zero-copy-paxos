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

#include <assert.h>
#include <atomic>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
#include <unistd.h>

int connect_to(const char *host, const char *port) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family   = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags    = AI_PASSIVE;

  struct addrinfo *ai;
  int getaddrinfo_result = getaddrinfo(host, port, &hints, &ai);
  if (getaddrinfo_result != 0) {
    fprintf(stderr, "getaddrinfo failed: %s\n", gai_strerror(getaddrinfo_result));
    return -1;
  }

  for (struct addrinfo *p = ai; p != NULL; p = p->ai_next) {
    int fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd == -1) {
      perror("client: socket");
      continue;
    }

    if (connect(fd, p->ai_addr, p->ai_addrlen) == -1) {
      perror("client: connect");
      close(fd);
      continue;
    }

    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
      perror("client: fcntl");
      close(fd);
      continue;
    }

    freeaddrinfo(ai);
    return fd;
  }

  fprintf(stderr, "%s: failed\n", __PRETTY_FUNCTION__);
  abort();
}

struct ReadThreadData {
  const int fd;
  std::atomic<uint64_t> total_read;
  std::atomic<uint32_t> total_received_acks;

  ReadThreadData(int fd)
    : fd(fd),
      total_read(0),
      total_received_acks(0) {}
};

void read_thread_main(ReadThreadData *d) {

  int epfd = epoll_create(1);
  if (epfd == -1) {
    perror("epoll_create()");
    abort();
  }

  struct epoll_event r_evt;
  r_evt.events    = EPOLLIN;
  r_evt.data.u64  = 0;

  if (epoll_ctl(epfd, EPOLL_CTL_ADD, d->fd, &r_evt) == -1) {
    perror("epoll_ctl()");
    abort();
  }

  uint32_t received_ack;
  size_t   received_bytes = 0;
  unsigned char *receive_buffer
    = reinterpret_cast<unsigned char*>(&received_ack);

  while (1) {
    struct epoll_event evt;
    int event_count = epoll_wait(epfd, &evt, 1, 100000);

    if (event_count == 0) {
      continue;
    }

    while (1) {
      int read_len = read(d->fd, receive_buffer + received_bytes,
                                sizeof(uint32_t) - received_bytes);
      if (read_len == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          break;
        } else {
          perror("read");
          abort();
        }

      } else if (read_len == 0) {
        printf("Server sent EOF\n");
        close(epfd);
        return;

      } else {
        assert(read_len >= 0);
        received_bytes += read_len;
        assert(received_bytes <= sizeof(uint32_t));

        if (received_bytes == sizeof(uint32_t)) {
          d->total_read          += received_ack;
          d->total_received_acks += 1;
          received_bytes = 0;
        }
      }
    }
  }
}

struct Statistics {
  uint64_t current_ela_time_sec;
  uint64_t current_ela_time_nsec;
  uint64_t current_usr_time_sec;
  uint64_t current_usr_time_usec;
  uint64_t current_sys_time_sec;
  uint64_t current_sys_time_usec;
  uint64_t written_byte_count;
  uint64_t ack_count;
  uint64_t acked_byte_count;
};

int main(int argc, char **argv) {
  int sock = connect_to("127.0.0.1", "41715");

  uint64_t total_txns    = 0;
  uint64_t total_written = 0;

  double n_samples = 0;
  double sum_t     = 0;
  double sum_b     = 0;
  double sum_w     = 0;
  double sum_tt    = 0;
  double sum_tb    = 0;
  double sum_tw    = 0;

  int epfd = epoll_create(1);
  if (epfd == -1) {
    perror("epoll_create()");
    abort();
  }

  struct epoll_event w_evt;
  w_evt.events   = EPOLLOUT;
  w_evt.data.u64 = 0;

  if (epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &w_evt) == -1) {
    perror("epoll_ctl()");
    abort();
  }

  bool is_writeable = false;

  struct timespec current_time;
  clock_gettime(CLOCK_MONOTONIC, &current_time);

  struct timespec last_output = current_time;
  last_output.tv_sec = 0;

  double bucket_capacity_bytes             = 1e5;
  double bucket_level_bytes                = bucket_capacity_bytes;
  double bucket_leak_rate_bytes_per_sec    = 1e5;
  struct timespec bucket_last_request_time = current_time;

  size_t request_size = 50;
  char *current_request_buf = (char*)malloc(request_size);
  if (current_request_buf == NULL) {
    fprintf(stderr, "malloc() failed\n");
    abort();
  }

  uint32_t current_request_number = 0;
  char    *current_request_ptr;
  size_t   current_request_still_to_send = 0;

  printf("Target rate: %f B/s\n", bucket_leak_rate_bytes_per_sec);
  printf("Request size: %lu B\n", request_size);

  printf("%19s %18s %18s %18s %18s %18s %18s %18s\n",
         "elapsed time (s)",
               "user time (s)",
                    "system time (s)",
                         "written (B)",
                              "acks",
                                  "acked (B)",
                                        "ack rate (B/s)",
                                             "txn rate (Hz)");

  ReadThreadData rtd(sock);
  std::thread read_thread(read_thread_main, &rtd);

  srand(time(NULL));
  while (1) {

    while (!is_writeable) {
      struct epoll_event evt;
      int event_count = epoll_wait(epfd, &evt, 1, 100000);
      if (event_count > 0 && evt.events & EPOLLOUT) {
        is_writeable = true;
      }
    }

    clock_gettime(CLOCK_MONOTONIC, &current_time);

    uint32_t total_received_acks = rtd.total_received_acks.load();
    uint64_t total_read          = rtd.total_read.load();

    if (last_output.tv_sec < current_time.tv_sec) {
      last_output = current_time;

      double t = (double)current_time.tv_sec
          + 1.0e-9 * ((double) current_time.tv_nsec);
      double b = total_read;
      double w = total_txns;

      n_samples += 1;
      sum_t     += t;
      sum_b     += b;
      sum_w     += w;
      sum_tt    += t * t;
      sum_tb    += t * b;
      sum_tw    += t * w;

      double rate_b = (n_samples * sum_tb - sum_t * sum_b)
                    / (n_samples * sum_tt - sum_t * sum_t);
      double rate_w = (n_samples * sum_tw - sum_t * sum_w)
                    / (n_samples * sum_tt - sum_t * sum_t);

      struct rusage usage;
      if (getrusage(RUSAGE_SELF, &usage) == -1) {
        perror("getrusage()");
        abort();
      }

      printf("%9ld.%09ld %11lu.%06lu %11lu.%06lu %18lu %18u %18lu %18.5e %18.5e\n",
        current_time.tv_sec, current_time.tv_nsec,
        usage.ru_utime.tv_sec, usage.ru_utime.tv_usec,
        usage.ru_stime.tv_sec, usage.ru_stime.tv_usec,
        total_written,
        total_received_acks, total_read,
        rate_b, rate_w);
    }

    double bucket_leaked_bytes
      = (  current_time.tv_sec  - bucket_last_request_time.tv_sec
        + (current_time.tv_nsec - bucket_last_request_time.tv_nsec) * 1.0e-9)
            * bucket_leak_rate_bytes_per_sec;
    if (bucket_level_bytes <= bucket_leaked_bytes) {
      bucket_level_bytes = 0.0;
    } else {
      bucket_level_bytes -= bucket_leaked_bytes;
    }
    bucket_last_request_time = current_time;

    int requests_this_time = 0;

    while (is_writeable && requests_this_time <= 10000) {
      if (current_request_still_to_send == 0) {
        total_txns             += 1;
        current_request_number += 1;
        requests_this_time     += 1;

#define REQUEST_TMP_BUF_SIZE 30
        char request_tmp_buf[REQUEST_TMP_BUF_SIZE];
        int snprintf_result = snprintf(request_tmp_buf, REQUEST_TMP_BUF_SIZE,
                                       "request%u\n", current_request_number);
        if (snprintf_result >= REQUEST_TMP_BUF_SIZE) {
          fprintf(stderr, "snprintf(request) overflowed\n");
          abort();
        }

        size_t buffer_filled_to = snprintf_result;
        if (buffer_filled_to >= request_size) {
          memmove(current_request_buf, request_tmp_buf, request_size);
        } else {
          memmove(current_request_buf, request_tmp_buf, snprintf_result);
          while (1) {
            if (2 * buffer_filled_to < request_size) {
              memmove(current_request_buf + buffer_filled_to, current_request_buf, buffer_filled_to);
              buffer_filled_to *= 2;
            } else {
              memmove(current_request_buf + buffer_filled_to, current_request_buf, request_size - buffer_filled_to);
              break;
            }
          }
        }

        current_request_ptr = current_request_buf;
        current_request_still_to_send = request_size;
      }

      if (bucket_level_bytes + current_request_still_to_send <= bucket_capacity_bytes) {
        bucket_level_bytes += current_request_still_to_send;
      } else {
        break;
      }

      int write_result = write(sock, current_request_ptr, current_request_still_to_send);
      if (write_result == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          perror("write");
          abort();
        } else {
          is_writeable = false;
        }
      } else {
        assert(write_result >= 0);
        size_t written_bytes = write_result;
        assert(written_bytes <= current_request_still_to_send);
        total_written += written_bytes;
        current_request_still_to_send -= written_bytes;
        bucket_level_bytes -= current_request_still_to_send;
      }
    }
  }

  close(sock);

  return 0;
}
