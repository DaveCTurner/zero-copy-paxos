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
#include <getopt.h>
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

struct option long_options[] =
  {
    {"host",         required_argument, 0, 'h'},
    {"port",         required_argument, 0, 'p'},
    {"rate",         required_argument, 0, 'r'},
    {"request-size", required_argument, 0, 's'},
    {0, 0, 0, 0}
  };

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

#define MAX_RECEIVED_ACKS 1024
  uint32_t received_acks[MAX_RECEIVED_ACKS];
  size_t   received_bytes = 0;
  unsigned char *receive_buffer
    = reinterpret_cast<unsigned char*>(received_acks);

  while (1) {
    struct epoll_event evt;
    int event_count = epoll_wait(epfd, &evt, 1, 100000);

    if (event_count == 0) {
      continue;
    }

    while (1) {
      int read_len = read(d->fd, receive_buffer + received_bytes,
                                sizeof(uint32_t) * MAX_RECEIVED_ACKS
                                    - received_bytes);
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
        assert(received_bytes <= sizeof(uint32_t) * MAX_RECEIVED_ACKS);

        for (uint32_t ack_index = 0;
                      ack_index < received_bytes/sizeof(uint32_t);
                      ack_index++) {
          d->total_read          += received_acks[ack_index];
          d->total_received_acks += 1;
        }

        size_t leftover = received_bytes % sizeof(uint32_t);
        if (leftover != 0) {
          received_acks[0] = received_acks[received_bytes/sizeof(uint32_t)];
        }
        received_bytes = leftover;
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
  const char *host        = NULL;
  const char *port        = NULL;
  const char *rate_string = NULL;
  const char *size_string = NULL;

  while (1) {
    int option_index = 0;
    int getopt_result = getopt_long(argc, argv, "h:p:r:s:",
                                    long_options, &option_index);

    if (getopt_result == -1) { break; }

    switch (getopt_result) {
      case 'h':
        if (host != NULL) {
          fprintf(stderr, "--host repeated\n");
          abort();
        }
        host = strdup(optarg);
        if (host == NULL) {
          perror("getopt: host");
          abort();
        }
        break;

      case 'p':
        if (port != NULL) {
          fprintf(stderr, "--port repeated\n");
          abort();
        }
        port = strdup(optarg);
        if (port == NULL) {
          perror("getopt: port");
          abort();
        }
        break;

      case 'r':
        if (rate_string != NULL) {
          fprintf(stderr, "--rate repeated\n");
          abort();
        }
        rate_string = strdup(optarg);
        if (rate_string == NULL) {
          perror("getopt: rate");
          abort();
        }
        break;

      case 's':
        if (size_string != NULL) {
          fprintf(stderr, "--request-size repeated\n");
          abort();
        }
        size_string = strdup(optarg);
        if (size_string == NULL) {
          perror("getopt: size");
          abort();
        }
        break;

      default:
        fprintf(stderr, "unknown option\n");
        abort();
    }
  }

  if (host == NULL) {
    fprintf(stderr, "--host required\n");
    abort();
  }

  if (port == NULL) {
    fprintf(stderr, "--port required\n");
    abort();
  }

  if (rate_string == NULL) {
    fprintf(stderr, "--rate required\n");
    abort();
  }

  if (size_string == NULL) {
    fprintf(stderr, "--request-size required\n");
    abort();
  }

  sleep(2);

  int sock = connect_to(host, port);

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

  double bucket_capacity_bytes             = 100e6;
  double bucket_level_bytes                = bucket_capacity_bytes;
  double bucket_leak_rate_bytes_per_sec    = strtod(rate_string, NULL);
  struct timespec bucket_last_request_time = current_time;

  size_t request_size = atoi(size_string);
  if (request_size == 0) {
    fprintf(stderr, "--request-size must be positive\n");
    abort();
  }
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

  struct timespec start_time      = current_time;
  bool            warmup_complete = false;

  ReadThreadData rtd(sock);
  std::thread read_thread(read_thread_main, &rtd);

  Statistics start_statistics;

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

      if (start_time.tv_sec + 60 < current_time.tv_sec) {
        printf("---- 60-sec run complete\n");
        shutdown(sock, SHUT_WR);
        read_thread.join();

        printf("%10s %18s %18s %18s %18s %18s %18s %18s %18s %18s\n",
                "",
                     "target rate",
                           "request size",
                                "start time",
                                      "end time",
                                         "elapsed (ms)",
                                              "user time (ms)",
                                                   "sys time (ms)",
                                                        "acks",
                                                             "acked (B)");
        printf("%10s %18f %18lu %8ld.%09ld %8ld.%09ld %18lu %18lu %18lu %18lu %18lu\n",
                "results:",
                bucket_leak_rate_bytes_per_sec,
                request_size,
                start_statistics.current_ela_time_sec,
                start_statistics.current_ela_time_nsec,
                current_time.tv_sec,
                current_time.tv_nsec,

                 (current_time.tv_sec * 1000 + current_time.tv_nsec / 1000000)
                - (start_statistics.current_ela_time_sec * 1000 +
                   start_statistics.current_ela_time_nsec / 1000000),

                  (usage.ru_utime.tv_sec * 1000 + usage.ru_utime.tv_usec / 1000)
                - (start_statistics.current_usr_time_sec * 1000 +
                   start_statistics.current_usr_time_usec / 1000),

                  (usage.ru_stime.tv_sec * 1000 + usage.ru_stime.tv_usec / 1000)
                - (start_statistics.current_sys_time_sec * 1000 +
                   start_statistics.current_sys_time_usec / 1000),

                   total_received_acks - start_statistics.ack_count,
                   total_read - start_statistics.acked_byte_count);

        exit(0);
      } else if (!warmup_complete && start_time.tv_sec + 15 < current_time.tv_sec) {
        printf("---- 15-sec warmup complete\n");
        warmup_complete = true;
        start_statistics.current_ela_time_sec  = current_time.tv_sec;
        start_statistics.current_ela_time_nsec = current_time.tv_nsec;
        start_statistics.current_usr_time_sec  = usage.ru_utime.tv_sec;
        start_statistics.current_usr_time_usec = usage.ru_utime.tv_usec;
        start_statistics.current_sys_time_sec  = usage.ru_stime.tv_sec;
        start_statistics.current_sys_time_usec = usage.ru_stime.tv_usec;
        start_statistics.written_byte_count    = total_written;
        start_statistics.ack_count             = total_received_acks;
        start_statistics.acked_byte_count      = total_read;
      }
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

