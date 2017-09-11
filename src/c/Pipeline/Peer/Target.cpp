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



#include "Pipeline/Peer/Target.h"
#include "Pipeline/Peer/Protocol.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>

namespace Pipeline {
namespace Peer {

Target::Address::Address(const char *host, const char *port)
      : host(host), port(port) { }

bool Target::is_connected() const { return fd != -1 && peer_id != 0; }

bool Target::is_connected_to(const Paxos::NodeId &n) const {
  return is_connected() && peer_id == n;
}

void Target::shutdown() {
  manager.deregister_close_and_clear(fd);
  received_handshake_bytes = 0;
  peer_id = 0;
}

void Target::start_connection() {
  if (fd != -1) {
    return;
  }

  sent_handshake = false;
  received_handshake_bytes = 0;
  waiting_to_become_writeable = false;
  peer_id = 0;

  struct addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *remote_addrinfo;
  int getaddrinfo_result
    = getaddrinfo(address.host.c_str(),
                  address.port.c_str(),
                  &hints, &remote_addrinfo);
  if (getaddrinfo_result != 0) {
    fprintf(stderr, "%s: getaddrinfo(remote) failed: %s\n",
      __PRETTY_FUNCTION__,
      gai_strerror(getaddrinfo_result));
    return;
  }

  for (struct addrinfo *r = remote_addrinfo; r != NULL; r = r->ai_next) {
    fd = socket(r->ai_family,
                r->ai_socktype | SOCK_NONBLOCK,
                r->ai_protocol);
    if (fd == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: socket() failed\n", __PRETTY_FUNCTION__);
      continue;
    }

    int connect_result = connect(fd, r->ai_addr, r->ai_addrlen);
    if (connect_result == 0) {
      handle_writeable();
      break;
    } else {
      assert(connect_result == -1);
      if (errno == EINPROGRESS) {
        manager.register_handler(fd, this, EPOLLOUT);
        waiting_to_become_writeable = true;
        break;
      } else {
        perror(__PRETTY_FUNCTION__);
        fprintf(stderr, "%s: connect() failed\n", __PRETTY_FUNCTION__);
        close(fd);
        fd = -1;
        continue;
      }
    }
  }

  freeaddrinfo(remote_addrinfo);
}

Target::Target(const Address           &address,
                     Epoll::Manager    &manager,
                     Paxos::Legislator &legislator,
               const NodeName          &node_name)
  : address(address),
    manager(manager),
    legislator(legislator),
    node_name(node_name) {
  start_connection();
}

void Target::handle_readable() {
  if (fd == -1) {
    return;
  }

  if (received_handshake_bytes < sizeof received_handshake) {
    switch(Protocol::receive_handshake(fd,
                                       received_handshake, received_handshake_bytes,
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
        printf("%s (fd=%d): connected to %d\n", __PRETTY_FUNCTION__,
                                                fd, peer_id);
#endif // ndef NTRACE
        break;
    }
  } else {
    fprintf(stderr, "%s (fd=%d): unexpected\n",
                    __PRETTY_FUNCTION__, fd);
    shutdown();
  }
}

void Target::handle_writeable() {
  if (fd == -1) {
    return;
  }

  if (!sent_handshake) {
    printf("%s (fd=%d): connected\n", __PRETTY_FUNCTION__, fd);
    assert(fd != -1);
    Protocol::send_handshake(fd, node_name);
    manager.modify_handler(fd, this, EPOLLIN);
    sent_handshake = true;
    return;
  }

  while (current_message.still_to_send > 0) {
    struct iovec iov[2];
    int          iovcnt;
    if (current_message.still_to_send <= sizeof(Protocol::Message)) {
      iovcnt = 1;
      iov[0].iov_len  = current_message.still_to_send;
      iov[0].iov_base = reinterpret_cast<uint8_t*>(&current_message.message)
                      + sizeof(Protocol::Message)
                      - current_message.still_to_send;
    } else {
      assert(current_message.still_to_send == sizeof(Protocol::Message)
                                            + 1);
      iovcnt = 2;
      iov[1].iov_len  = sizeof(Protocol::Message);
      iov[0].iov_len  = 1;
      iov[1].iov_base = reinterpret_cast<uint8_t*>(&current_message.message);
      iov[0].iov_base = reinterpret_cast<uint8_t*>(&current_message.type);
    }

#ifndef NTRACE
    size_t total_len = 0;
    printf("%s (fd=%d): sending", __PRETTY_FUNCTION__, fd);
    for (int i = 0; i < iovcnt; i++) {
      printf(" /");
      uint8_t *p = reinterpret_cast<uint8_t*>(iov[i].iov_base);
      total_len += iov[i].iov_len;
      for (size_t j = 0; j < iov[i].iov_len; j++) {
        printf(" %02x", *p);
        p++;
      }
    }
    printf(" = %lu bytes\n", total_len);
#endif // ndef NTRACE

    ssize_t writev_result = writev(fd, iov, iovcnt);

#ifndef NTRACE
    printf("%s: writev returned %ld\n", __PRETTY_FUNCTION__, writev_result);
#endif // ndef NTRACE

    if (writev_result == -1) {
      if (errno == EAGAIN) {
        if (!waiting_to_become_writeable) {
          manager.modify_handler(fd, this, EPOLLOUT);
          waiting_to_become_writeable = true;
        }
      } else {
        perror(__PRETTY_FUNCTION__);
        fprintf(stderr, "%s: writev() failed\n", __PRETTY_FUNCTION__);
        shutdown();
      }
      return;
    } else {
      assert(writev_result >= 0);
      size_t bytes_written = writev_result;
      assert(bytes_written <= current_message.still_to_send);
      current_message.still_to_send -= bytes_written;
    }
  }

  if (waiting_to_become_writeable) {
    manager.modify_handler(fd, this, 0);
    waiting_to_become_writeable = false;
  }
}

void Target::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
  assert(fd == -1);
}

bool Target::prepare_to_send(uint8_t message_type) {
  if (!is_connected()) {
    return false;
  }
  if (0 < current_message.still_to_send) {
    return false;
  }

  assert(current_message.still_to_send == 0);
  memset(&current_message, 0, sizeof(current_message));
  current_message.type          = message_type;
  current_message.still_to_send = 1 + sizeof(Protocol::Message);
  return true;
}

void Target::seek_votes_or_catch_up(const Paxos::Slot &first_unchosen_slot,
                            const Paxos::Term &min_acceptable_term) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << first_unchosen_slot
            << " " << min_acceptable_term
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_SEEK_VOTES_OR_CATCH_UP)) { return; }
  auto &payload = current_message.message.seek_votes_or_catch_up;
  payload.slot = first_unchosen_slot;
  payload.term.copy_from(min_acceptable_term);
  handle_writeable();
}

void Target::offer_vote(const Paxos::NodeId &destination,
                const Paxos::Term   &min_acceptable_term) {
  if (!is_connected_to(destination)) { return; }
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << destination
            << " " << min_acceptable_term
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_OFFER_VOTE)) { return; }
  auto &payload = current_message.message.offer_vote;
  payload.term.copy_from(min_acceptable_term);
  handle_writeable();
}

void Target::offer_catch_up(const Paxos::NodeId &destination) {
  if (!is_connected_to(destination)) { return; }
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << destination
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_OFFER_CATCH_UP)) { return; }
  handle_writeable();
}

void Target::request_catch_up(const Paxos::NodeId &destination) {
  if (!is_connected_to(destination)) { return; }
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << destination
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_REQUEST_CATCH_UP)) { return; }
  handle_writeable();
}

void Target::send_catch_up(
  const Paxos::NodeId&          destination,
  const Paxos::Slot&            first_unchosen_slot,
  const Paxos::Era&             current_era,
  const Paxos::Configuration&   current_configuration,
  const Paxos::NodeId&          next_generated_node_id,
  const Paxos::Value::StreamName& current_stream,
  const uint64_t                current_stream_pos) {

  if (!is_connected_to(destination)) { return; }
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << destination
            << " " << first_unchosen_slot
            << " " << current_era
            << " " << current_configuration
            << " " << next_generated_node_id
            << " " << current_stream
            << " " << current_stream_pos
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_SEND_CATCH_UP)) { return; }
  auto &payload = current_message.message.send_catch_up;
  payload.slot                    = first_unchosen_slot;
  payload.era                     = current_era;
  payload.next_generated_node_id  = next_generated_node_id;
  payload.current_stream_owner    = current_stream.owner;
  payload.current_stream_id       = current_stream.id;
  payload.current_stream_position = current_stream_pos;
  payload.configuration_size      = current_configuration.entries.size();

  handle_writeable();
  if (current_message.still_to_send > 0) {
    fprintf(stderr, "%s: partial write of catch-up data, %ld bytes remaining\n",
      __PRETTY_FUNCTION__, current_message.still_to_send);
    shutdown();
    return;
  }

  if (!is_connected()) {
    fprintf(stderr, "%s: disconnected before writing configuration entries\n",
                     __PRETTY_FUNCTION__);
    return;
  }

  for (const auto &entry : current_configuration.entries) {

    Protocol::Message::configuration_entry e;
    e.node_id = entry.node_id();
    e.weight  = entry.weight();
    const uint8_t* ptr  = reinterpret_cast<const uint8_t*>(&e);
    const size_t   size = sizeof e;

#ifndef NTRACE
    printf("%s: sending %lu bytes:", __PRETTY_FUNCTION__, size);
    for (size_t n = 0; n < size; n++) {
      printf(" %02x", ptr[n]);
    }
    printf("\n");
#endif // ndef NTRACE

    ssize_t write_result = write(fd, ptr, size);

    if (write_result == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: write() failed\n", __PRETTY_FUNCTION__);
      shutdown();
      return;
    } else {
      assert(0 <= write_result);
      size_t bytes_written = write_result;
      if (bytes_written < size) {
        fprintf(stderr, "%s: write() only wrote %ld of %ld bytes\n",
               __PRETTY_FUNCTION__, bytes_written, size);
        shutdown();
        return;
      }
    }
  }
}

}}
