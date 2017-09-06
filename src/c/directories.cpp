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



#include "directories.h"

#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>

void ensure_directory(const char *parent, const char *path) {
  struct stat buf;
  if (stat(path, &buf) == -1) {
    if (errno == ENOENT) {
      if (mkdir(path, 0755) == -1) {
        perror(__PRETTY_FUNCTION__);
        fprintf(stderr, "%s: failed to mkdir(%s)\n",
                        __PRETTY_FUNCTION__, path);
        abort();
      }
      sync_directory(parent);

    } else {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: failed to stat(%s)\n",
                      __PRETTY_FUNCTION__, path);
      abort();
    }
  } else {
    if (!S_ISDIR(buf.st_mode)) {
      fprintf(stderr, "%s: %s is not a directory\n",
                      __PRETTY_FUNCTION__, path);
      abort();
    }
  }
}

void ensure_length(int snprintf_result) {
  if (snprintf_result >= PATH_MAX) {
    fprintf(stderr, "%s: path overflowed buffer of length %d\n",
                     __PRETTY_FUNCTION__, PATH_MAX);
    abort();
  }
}

void sync_directory(const char *path) {
  int fd = open(path, 0);
  if (fd == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: open(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }

  if (fsync(fd) == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: fsync(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }

  if (close(fd) == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s: fsync(%s) failed\n", __PRETTY_FUNCTION__, path);
    abort();
  }
}
