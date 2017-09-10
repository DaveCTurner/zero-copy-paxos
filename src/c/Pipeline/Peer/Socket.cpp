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



#include "Pipeline/Peer/Socket.h"
#include "Pipeline/Pipe.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"

#include <fcntl.h>
#include <limits.h>
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

namespace Pipeline {
namespace Peer {

void Socket::shutdown() {
  manager.deregister_close_and_clear(fd);
}

Socket::Socket
       (Epoll::Manager                  &manager,
        Paxos::Legislator               &legislator,
        const NodeName                  &node_name,
        const int                        fd)
  : manager         (manager),
    legislator      (legislator),
    node_name       (node_name),
    fd              (fd) {

  manager.register_handler(fd, this, EPOLLIN);

  Protocol::send_handshake(fd, node_name);
}

Socket::~Socket() {
  shutdown();
}

bool Socket::is_shutdown() const {
  return fd == -1;
}

void Socket::handle_readable() {
  assert(fd != -1);

  if (received_handshake_size < sizeof received_handshake) {
    assert(peer_id == 0);

    switch(Protocol::receive_handshake(fd,
                                       received_handshake,
                                       received_handshake_size,
                                       node_name.cluster)) {

      case RECEIVE_HANDSHAKE_ERROR:
      case RECEIVE_HANDSHAKE_EOF:
      case RECEIVE_HANDSHAKE_INVALID:
        shutdown();
        break;

      case RECEIVE_HANDSHAKE_INCOMPLETE:
        break;

      case RECEIVE_HANDSHAKE_SUCCESS:
        peer_id = received_handshake.node_id;

#ifndef NTRACE
        printf("%s (fd=%d): accepted handshake version %d cluster %s node %d\n",
          __PRETTY_FUNCTION__, fd,
          received_handshake.protocol_version,
          received_handshake.cluster_id,
          received_handshake.node_id);
#endif // ndef NTRACE
        break;
    }

    return;
  }

  struct iovec iov[2];
  int iovcnt;
  if (size_received == 0) {
    iovcnt = 2;
    iov[0].iov_base = reinterpret_cast<uint8_t*>(&current_message_type);
    iov[1].iov_base = reinterpret_cast<uint8_t*>(&current_message);
    iov[0].iov_len  = 1;
    iov[1].iov_len  = sizeof(Protocol::Message);
  } else {
    assert(size_received < 1 + sizeof(Protocol::Message));
    iovcnt = 1;
    iov[0].iov_base = reinterpret_cast<uint8_t*>(&current_message)
                    + (size_received - 1);
    iov[0].iov_len  = sizeof(Protocol::Message)
                    - (size_received - 1);
  }

  int readv_result = readv(fd, iov, iovcnt);
  if (readv_result == -1) {
    perror(__PRETTY_FUNCTION__);
    fprintf(stderr, "%s (fd=%d,peer=%d): readv() failed\n",
                    __PRETTY_FUNCTION__, fd, peer_id);
    shutdown();
    return;
  }

  if (readv_result == 0) {
#ifndef NTRACE
    printf("%s (fd=%d,peer=%d): EOF in readv()\n",
            __PRETTY_FUNCTION__, fd, peer_id);
#endif // ndef NTRACE
    shutdown();
    return;
  }

  assert(readv_result > 0);
  size_received += readv_result;
  assert(size_received <= 1 + sizeof(Protocol::Message));

  if (size_received < 1 + sizeof(Protocol::Message)) {
    return;
  }

#ifndef NTRACE
  printf("%s (fd=%d,peer=%d): receiving message type=%02x\n",
    __PRETTY_FUNCTION__, fd, peer_id,
    current_message_type);
#endif // ndef NTRACE

  switch (current_message_type) {

    case MESSAGE_TYPE_SEEK_VOTES_OR_CATCH_UP:
    {
      const auto &payload = current_message.seek_votes_or_catch_up;
      const auto term = payload.term.get_paxos_term();
#ifndef NTRACE
      std::cout << __PRETTY_FUNCTION__
        << " (fd=" << fd << ",peer=" << peer_id << "): "
        << "received seek_votes_or_catch_up("
        << payload.slot << ", "
        << term << ")"
        << std::endl;
#endif // ndef NTRACE
      legislator.handle_seek_votes_or_catch_up(peer_id,
           payload.slot,
           term);
      size_received = 0;
      return;
    }

    case MESSAGE_TYPE_OFFER_CATCH_UP:
    {
#ifndef NTRACE
      std::cout << __PRETTY_FUNCTION__
        << " (fd=" << fd << ",peer=" << peer_id << "): "
        << "received offer_catch_up()"
        << std::endl;
#endif // ndef NTRACE
      legislator.handle_offer_catch_up(peer_id);
      size_received = 0;
      return;
    }

    default:
      fprintf(stderr, "%s (fd=%d): unknown message type=%02x\n",
          __PRETTY_FUNCTION__, fd,
          current_message_type);
      shutdown();
      return;
  }
}

void Socket::handle_writeable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  abort();
}

void Socket::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
}

}
}
