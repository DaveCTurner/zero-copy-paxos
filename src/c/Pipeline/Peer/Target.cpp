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
  assert(fd == -1);
  received_handshake_bytes = 0;
  peer_id = 0;
}

void Target::start_connection() {
#ifndef NTRACE
  printf("%s (fd=%d): called\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

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

#ifndef NTRACE
  printf("%s (fd=%d): getaddrinfo succeeded\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

  for (struct addrinfo *r = remote_addrinfo; r != NULL; r = r->ai_next) {
    fd = socket(r->ai_family,
                r->ai_socktype | SOCK_NONBLOCK,
                r->ai_protocol);
    if (fd == -1) {
      perror(__PRETTY_FUNCTION__);
      fprintf(stderr, "%s: socket() failed\n", __PRETTY_FUNCTION__);
      continue;
    }

#ifndef NTRACE
    printf("%s (fd=%d): created socket\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

    int connect_result = connect(fd, r->ai_addr, r->ai_addrlen);
    if (connect_result == 0) {
#ifndef NTRACE
      printf("%s: connect() succeeded\n", __PRETTY_FUNCTION__);
#endif // ndef NTRACE
      handle_writeable();
      break;
    } else {
      assert(connect_result == -1);
      if (errno == EINPROGRESS) {
#ifndef NTRACE
        printf("%s: connect() returned EINPROGRESS\n", __PRETTY_FUNCTION__);
#endif // ndef NTRACE
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

uint8_t Target::value_type(const Paxos::Value::Type &t) {
  switch (t) {
    case Paxos::Value::Type::no_op:               return VALUE_TYPE_NO_OP;
    case Paxos::Value::Type::generate_node_id:    return VALUE_TYPE_GENERATE_NODE_ID;
    case Paxos::Value::Type::reconfiguration_inc: return VALUE_TYPE_INCREMENT_WEIGHT;
    case Paxos::Value::Type::reconfiguration_dec: return VALUE_TYPE_DECREMENT_WEIGHT;
    case Paxos::Value::Type::reconfiguration_mul: return VALUE_TYPE_MULTIPLY_WEIGHTS;
    case Paxos::Value::Type::reconfiguration_div: return VALUE_TYPE_DIVIDE_WEIGHTS;
    case Paxos::Value::Type::stream_content:      return VALUE_TYPE_STREAM_CONTENT;
  }
  fprintf(stderr, "%s: bad value: %d", __PRETTY_FUNCTION__, t);
  abort();
}

void Target::set_current_message_value(const Paxos::Value &value) {
  Protocol::Value &v = current_message.value;
  switch (value.type) {
    case Paxos::Value::Type::no_op:
      return;
    case Paxos::Value::Type::generate_node_id:
      v.generate_node_id.originator = value.payload.originator;
      return;
    case Paxos::Value::Type::reconfiguration_inc:
      v.increment_weight.node_id = value.payload.reconfiguration.subject;
      return;
    case Paxos::Value::Type::reconfiguration_dec:
      v.decrement_weight.node_id = value.payload.reconfiguration.subject;
      return;
    case Paxos::Value::Type::reconfiguration_mul:
      v.multiply_weights.multiplier = value.payload.reconfiguration.factor;
      return;
    case Paxos::Value::Type::reconfiguration_div:
      v.divide_weights.divisor = value.payload.reconfiguration.factor;
      return;
    case Paxos::Value::Type::stream_content:
      v.stream_content.stream_owner  = value.payload.stream.name.owner;
      v.stream_content.stream_id     = value.payload.stream.name.id;
      v.stream_content.stream_offset = value.payload.stream.offset;
      return;
  }
  fprintf(stderr, "%s: bad value type: %d", __PRETTY_FUNCTION__, value.type);
  abort();
}

Target::Target(const Address           &address,
                     Epoll::Manager    &manager,
                     SegmentCache      &segment_cache,
                     Paxos::Legislator &legislator,
               const NodeName          &node_name)
  : streaming_slots(0,0),
    address(address),
    manager(manager),
    segment_cache(segment_cache),
    legislator(legislator),
    node_name(node_name) {
  start_connection();
}

void Target::handle_readable() {
  if (fd == -1) {
    return;
  }

  if (received_handshake_bytes < sizeof received_handshake) {
    assert(peer_id == 0);

    switch(Protocol::receive_handshake(fd,
                                       received_handshake,
                                       received_handshake_bytes,
                                       node_name.cluster)) {

      case RECEIVE_HANDSHAKE_ERROR:
        fprintf(stderr, "%s (fd=%d): read(handshake) failed\n",
                        __PRETTY_FUNCTION__, fd);
        shutdown();
        return;

      case RECEIVE_HANDSHAKE_INCOMPLETE:
        return;

      case RECEIVE_HANDSHAKE_EOF:
#ifndef NTRACE
        printf("%s (fd=%d): EOF in handshake\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
        shutdown();
        return;

      case RECEIVE_HANDSHAKE_INVALID:
#ifndef NTRACE
        printf("%s (fd=%d): invalid handshake\n", __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
        shutdown();
        return;

      case RECEIVE_HANDSHAKE_SUCCESS:
        peer_id = received_handshake.node_id;

#ifndef NTRACE
        printf("%s (fd=%d): accepted handshake version %d cluster %s node %d\n",
          __PRETTY_FUNCTION__, fd,
          received_handshake.protocol_version,
          received_handshake.cluster_id,
          received_handshake.node_id);
#endif // ndef NTRACE
        assert(is_connected());
        return;
    }

    fprintf(stderr, "%s (fd=%d): unexpected result from receive_handshake\n",
      __PRETTY_FUNCTION__, fd);
    abort();
  }

  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  shutdown();
}

void Target::handle_writeable() {
#ifndef NTRACE
  fprintf(stderr, "%s (fd=%d,peer=%d)\n", __PRETTY_FUNCTION__, fd, peer_id);
#endif // ndef NTRACE

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

  if (peer_id == 0) {
    return;
  }

  bool sent_data = false;

  while (current_message.still_to_send > 0) {
    struct iovec iov[3];
    int          iovcnt;
    if (current_message.still_to_send <= sizeof(Protocol::Value)) {
      iovcnt = 1;
      iov[0].iov_len  = current_message.still_to_send;
      iov[0].iov_base = reinterpret_cast<uint8_t*>(&current_message.value)
                      + sizeof(Protocol::Value)
                      - current_message.still_to_send;
    } else if (current_message.still_to_send <= sizeof(Protocol::Value)
                                              + sizeof(Protocol::Message)) {

      iovcnt = 2;
      iov[1].iov_len  = sizeof(Protocol::Value);
      iov[0].iov_len  = current_message.still_to_send
                      - sizeof(Protocol::Value);
      iov[1].iov_base = reinterpret_cast<uint8_t*>(&current_message.value);
      iov[0].iov_base = reinterpret_cast<uint8_t*>(&current_message.message)
                      + sizeof(Protocol::Message)
                      + sizeof(Protocol::Value)
                      - current_message.still_to_send;
    } else {
      assert(current_message.still_to_send == sizeof(Protocol::Value)
                                            + sizeof(Protocol::Message)
                                            + 1);
      iovcnt = 3;
      iov[2].iov_len  = sizeof(Protocol::Value);
      iov[1].iov_len  = sizeof(Protocol::Message);
      iov[0].iov_len  = 1;
      iov[2].iov_base = reinterpret_cast<uint8_t*>(&current_message.value);
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
      if (writev_result > 0) {
        sent_data = true;
      }
    }
  }

  assert (current_message.still_to_send == 0);

  if (waiting_to_become_writeable) {
    manager.modify_handler(fd, this, 0);
    waiting_to_become_writeable = false;
  }

  if (sent_data) {
    // Just finished sending a message, so may need to switch to another mode.

    if (current_message.type == MESSAGE_TYPE_START_STREAMING_PROMISES) {

#ifndef NTRACE
      printf("%s (fd=%d): sent MESSAGE_TYPE_START_STREAMING_PROMISES\n",
        __PRETTY_FUNCTION__, fd);
      printf("%s (fd=%d): active senders before cleanout = %ld\n",
        __PRETTY_FUNCTION__, fd, bound_promise_senders.size());
#endif // ndef NTRACE

      bound_promise_senders.erase(std::remove_if(
        bound_promise_senders.begin(),
        bound_promise_senders.end(),
        [](const std::unique_ptr<BoundPromiseSender> &s) {
          return s->is_shutdown(); }),
        bound_promise_senders.end());

#ifndef NTRACE
      printf("%s (fd=%d): active senders after cleanout = %ld\n",
        __PRETTY_FUNCTION__, fd, bound_promise_senders.size());
#endif // ndef NTRACE

      bound_promise_senders.push_back(
        std::move(std::unique_ptr<BoundPromiseSender>
          (new BoundPromiseSender(manager, segment_cache,
                                  node_name, fd,
                                  streaming_slots, streaming_stream))));

      // previous constructor took ownership of this FD so dissociate it and
      // make a new one.
      fd = -1;
      start_connection();

    } else if (current_message.type == MESSAGE_TYPE_START_STREAMING_PROPOSALS) {

#ifndef NTRACE
      printf("%s (fd=%d): active senders before cleanout = %ld\n",
        __PRETTY_FUNCTION__, fd, expired_proposed_and_accepted_senders.size());
#endif // ndef NTRACE

      expired_proposed_and_accepted_senders.erase(std::remove_if(
        expired_proposed_and_accepted_senders.begin(),
        expired_proposed_and_accepted_senders.end(),
        [](const std::unique_ptr<ProposedAndAcceptedSender> &s) {
          return s->is_shutdown(); }),
        expired_proposed_and_accepted_senders.end());

#ifndef NTRACE
      printf("%s (fd=%d): active senders after cleanout = %ld\n",
        __PRETTY_FUNCTION__, fd, expired_proposed_and_accepted_senders.size());
#endif // ndef NTRACE

      assert(current_proposed_and_accepted_sender == NULL);

#ifndef NTRACE
      printf("%s (fd=%d): creating new sender\n",
        __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

      current_proposed_and_accepted_sender
        = std::unique_ptr<ProposedAndAcceptedSender>
          (new ProposedAndAcceptedSender(manager, segment_cache,
                                         node_name, fd,
                                         streaming_slots, streaming_stream));

      // previous constructor took ownership of this FD so dissociate it and
      // make a new one.
      fd = -1;
      start_connection();
    }
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
#ifndef NTRACE
    printf("%s (fd=%d,type=%02x): not connected\n",
      __PRETTY_FUNCTION__, fd, message_type);
#endif //ndef NTRACE
    return false;
  }
  if (0 < current_message.still_to_send) {
#ifndef NTRACE
    printf("%s (fd=%d,type=%02x): still %ld bytes of previous message (%02x) to send\n",
          __PRETTY_FUNCTION__, fd, message_type,
          current_message.still_to_send, current_message.type);
#endif //ndef NTRACE
    return false;
  }

  assert(current_message.still_to_send == 0);
  memset(&current_message, 0, sizeof(current_message));
  current_message.type          = message_type;
  current_message.still_to_send = 1 + sizeof(Protocol::Message)
                                    + sizeof(Protocol::Value);
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

void Target::prepare_term(const Paxos::Term &term) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << term
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_PREPARE_TERM)) { return; }
  auto &payload = current_message.message.prepare_term;
  payload.term.copy_from(term);
  handle_writeable();
}

void Target::make_promise(const Paxos::Promise &promise) {
  if (!is_connected_to(promise.term.owner)) { return; }

  if (promise.type == Paxos::Promise::Type::multi) {
#ifndef NTRACE
    std::cout << __PRETTY_FUNCTION__ << " (multi):"
              << " " << promise
              << std::endl;
#endif //ndef NTRACE
    if (!prepare_to_send(MESSAGE_TYPE_MAKE_PROMISE_MULTI)) { return; }
    auto &payload = current_message.message.make_promise_multi;
    payload.slot = promise.slots.start();
    payload.term.copy_from(promise.term);
    handle_writeable();
    return;
  }

  if (promise.slots.is_empty()) {
    return;
  }

  if (promise.type == Paxos::Promise::Type::free) {
#ifndef NTRACE
    std::cout << __PRETTY_FUNCTION__ << " (free):"
              << " " << promise
              << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send(MESSAGE_TYPE_MAKE_PROMISE_FREE)) { return; }
    auto &payload = current_message.message.make_promise_free;
    payload.start_slot = promise.slots.start();
    payload.end_slot   = promise.slots.end();
    payload.term.copy_from(promise.term);
    handle_writeable();
    return;
  }

  if (promise.type == Paxos::Promise::Type::bound) {
    if (promise.max_accepted_term_value.type == Paxos::Value::Type::stream_content) {
#ifndef NTRACE
      std::cout << __PRETTY_FUNCTION__ << " (start streaming):"
                << " " << promise
                << std::endl;
#endif //ndef NTRACE
      if (!prepare_to_send(MESSAGE_TYPE_START_STREAMING_PROMISES)) { return; }
      auto &pl = current_message.message.start_streaming_promises;
      auto &stream = promise.max_accepted_term_value.payload.stream;
      pl.stream_owner  = stream.name.owner;
      pl.stream_id     = stream.name.id;
      pl.stream_offset = stream.offset;
      pl.first_slot    = promise.slots.start();
      pl.term.copy_from(promise.term);
      pl.max_accepted_term.copy_from(promise.max_accepted_term);
      streaming_slots = promise.slots;
      streaming_stream = stream;
      handle_writeable();
      return;

    } else {
#ifndef NTRACE
      std::cout << __PRETTY_FUNCTION__ << " (bound):"
                << " " << promise
                << std::endl;
#endif //ndef NTRACE
      if (!prepare_to_send( MESSAGE_TYPE_MAKE_PROMISE_BOUND
                          | value_type(promise.max_accepted_term_value.type)))
             { return; }
      auto &payload = current_message.message.make_promise_bound;
      payload.start_slot = promise.slots.start();
      payload.end_slot   = promise.slots.end();
      payload.term.copy_from(promise.term);
      payload.max_accepted_term.copy_from(promise.max_accepted_term);
      set_current_message_value(promise.max_accepted_term_value);
      handle_writeable();
      return;
    }
  }

  fprintf(stderr, "%s: bad promise: %d", __PRETTY_FUNCTION__, promise.type);
  abort();
}

void Target::proposed_and_accepted(const Paxos::Proposal &proposal) {
  if (proposal.value.type == Paxos::Value::Type::stream_content) {

#ifndef NTRACE
    printf("%s (fd=%d): proposed_and_accepted(stream_content)\n",
      __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

    if (current_proposed_and_accepted_sender != NULL) {
#ifndef NTRACE
      printf("%s (fd=%d): try existing sender\n",
        __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

      if (current_proposed_and_accepted_sender->send(
            proposal.value.payload.stream, proposal.slots)) {
#ifndef NTRACE
        printf("%s (fd=%d): succeeded with existing sender\n",
          __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE
        return;
      }

#ifndef NTRACE
      printf("%s (fd=%d): failed with existing sender - expiring it\n",
        __PRETTY_FUNCTION__, fd);
#endif // ndef NTRACE

      expired_proposed_and_accepted_senders.push_back(
        std::move(current_proposed_and_accepted_sender));
    }

    assert(current_proposed_and_accepted_sender == NULL);

    if (!prepare_to_send(MESSAGE_TYPE_START_STREAMING_PROPOSALS)) { return; }
    auto &pl = current_message.message.start_streaming_proposals;
    auto &stream = proposal.value.payload.stream;
    pl.stream_owner  = stream.name.owner;
    pl.stream_id     = stream.name.id;
    pl.stream_offset = stream.offset;
    pl.first_slot    = proposal.slots.start();
    pl.term.copy_from(proposal.term);
    streaming_slots = proposal.slots;
    streaming_stream = stream;
  } else {
#ifndef NTRACE
    std::cout << __PRETTY_FUNCTION__ << ":"
              << " " << proposal
              << std::endl;
#endif //ndef NTRACE
    if (!prepare_to_send( MESSAGE_TYPE_PROPOSED_AND_ACCEPTED
                        | value_type(proposal.value.type)))
           { return; }
    auto &payload = current_message.message.proposed_and_accepted;
    payload.start_slot = proposal.slots.start();
    payload.end_slot   = proposal.slots.end();
    payload.term.copy_from(proposal.term);
    set_current_message_value(proposal.value);
  }
  handle_writeable();
}

void Target::accepted(const Paxos::Proposal &proposal) {
#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ":"
            << " " << proposal
            << std::endl;
#endif //ndef NTRACE
  if (!prepare_to_send( MESSAGE_TYPE_ACCEPTED
                      | value_type(proposal.value.type)))
         { return; }
  auto &payload = current_message.message.accepted;
  payload.start_slot = proposal.slots.start();
  payload.end_slot   = proposal.slots.end();
  payload.term.copy_from(proposal.term);
  set_current_message_value(proposal.value);
  handle_writeable();
}

Target::BoundPromiseSender::BoundPromiseSender(
        Epoll::Manager             &manager,
        SegmentCache               &segment_cache,
  const NodeName                   &node_name,
        int                         fd,
  const Paxos::SlotRange           &slots,
  const Paxos::Value::OffsetStream &stream)
  : manager(manager),
    segment_cache(segment_cache),
    fd(fd),
    slots(slots),
    stream(stream) {
  manager.modify_handler(fd, this, EPOLLOUT);
}

Target::BoundPromiseSender::~BoundPromiseSender() {
  shutdown();
}

void Target::BoundPromiseSender::shutdown() {
  manager.deregister_close_and_clear(fd);
  assert(fd == -1);
}

bool Target::BoundPromiseSender::is_shutdown() const {
  return fd == -1;
}

void Target::BoundPromiseSender::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
}

void Target::BoundPromiseSender::handle_readable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  shutdown();
}

void Target::BoundPromiseSender::handle_writeable() {
#ifndef NTRACE
  printf("%s (fd=%d): writing [%lu,%lu)\n",
    __PRETTY_FUNCTION__, fd, slots.start(), slots.end());
#endif // def NTRACE

  if (fd == -1) {
    return;
  }

  auto write_result
    = segment_cache.write_accepted_data_to(fd, stream, slots);

  switch(write_result) {
    case SegmentCache::WriteAcceptedDataResult::succeeded:
#ifndef NTRACE
      printf("%s (fd=%d): still-to-write [%lu,%lu)\n",
        __PRETTY_FUNCTION__, fd, slots.start(), slots.end());
#endif // def NTRACE
      if (slots.is_empty()) {
        shutdown();
      }
      break;

    case SegmentCache::WriteAcceptedDataResult::blocked:
      // Should not happen, as this is only called in response
      // to epoll_wait() returning EPOLLOUT. Fall through to ...

    case SegmentCache::WriteAcceptedDataResult::failed:
#ifndef NTRACE
      printf("%s (fd=%d): write failed, shutting down\n",
        __PRETTY_FUNCTION__, fd);
#endif // def NTRACE
      shutdown();
      break;
  }
}



Target::ProposedAndAcceptedSender::ProposedAndAcceptedSender(
        Epoll::Manager             &manager,
        SegmentCache               &segment_cache,
  const NodeName                   &node_name,
        int                         fd,
  const Paxos::SlotRange           &slots,
  const Paxos::Value::OffsetStream &stream)
  : manager(manager),
    segment_cache(segment_cache),
    fd(fd),
    slots(slots),
    stream(stream) {
  assert(slots.is_nonempty());
  manager.modify_handler(fd, this, EPOLLOUT);
}

Target::ProposedAndAcceptedSender::~ProposedAndAcceptedSender() {
  shutdown();
}

void Target::ProposedAndAcceptedSender::shutdown() {
  manager.deregister_close_and_clear(fd);
  assert(fd == -1);
}

bool Target::ProposedAndAcceptedSender::is_shutdown() const {
  return fd == -1;
}

void Target::ProposedAndAcceptedSender::handle_error(const uint32_t events) {
  fprintf(stderr, "%s (fd=%d, events=%x): unexpected\n",
                  __PRETTY_FUNCTION__, fd, events);
  shutdown();
}

void Target::ProposedAndAcceptedSender::handle_readable() {
  fprintf(stderr, "%s (fd=%d): unexpected\n",
                  __PRETTY_FUNCTION__, fd);
  shutdown();
}

void Target::ProposedAndAcceptedSender::handle_writeable() {
#ifndef NTRACE
  printf("%s (fd=%d): writing [%lu,%lu)\n",
    __PRETTY_FUNCTION__, fd, slots.start(), slots.end());
#endif // def NTRACE

  if (fd == -1) {
    return;
  }

  auto write_result
    = segment_cache.write_accepted_data_to(fd, stream, slots);

  switch(write_result) {
    case SegmentCache::WriteAcceptedDataResult::succeeded:
#ifndef NTRACE
      printf("%s (fd=%d): still-to-write [%lu,%lu)\n",
        __PRETTY_FUNCTION__, fd, slots.start(), slots.end());
#endif // def NTRACE
      if (waiting_to_be_writeable && slots.is_empty()) {
        waiting_to_be_writeable = false;
        manager.modify_handler(fd, this, 0);
      }
      break;

    case SegmentCache::WriteAcceptedDataResult::blocked:
#ifndef NTRACE
      printf("%s (fd=%d): blocked, still-to-write [%lu,%lu)\n",
        __PRETTY_FUNCTION__, fd, slots.start(), slots.end());
#endif // def NTRACE
      if (!waiting_to_be_writeable) {
        waiting_to_be_writeable = true;
        manager.modify_handler(fd, this, EPOLLOUT);
      }
      break;

    case SegmentCache::WriteAcceptedDataResult::failed:
#ifndef NTRACE
      printf("%s (fd=%d): write failed, shutting down\n",
        __PRETTY_FUNCTION__, fd);
#endif // def NTRACE
      shutdown();
       break;
  }
}

bool Target::ProposedAndAcceptedSender::send(
  const Paxos::Value::OffsetStream &proposal_stream,
  const Paxos::SlotRange           &proposal_slots) {

#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << " (fd=%d):"
    << " proposal_stream = " << proposal_stream
    << " proposal_slots = "  << proposal_slots
    << " stream = " << stream
    << " slots = "  << slots
    << " waiting_to_be_writeable = " << waiting_to_be_writeable
    << std::endl;
#endif // ndef NTRACE

  assert(proposal_slots.is_nonempty());

  if  (proposal_stream.name.owner != stream.name.owner
    || proposal_stream.name.id    != stream.name.id
    || proposal_stream.offset     != stream.offset
    || proposal_slots.start()     != slots.end()) {
      return false;
  }

  slots.set_end(proposal_slots.end());

  if (!waiting_to_be_writeable) {
    handle_writeable();
  }

  return true;
}


}}
