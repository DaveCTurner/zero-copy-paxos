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



#include "Paxos/Legislator.h"

using namespace Paxos;

Legislator::Legislator(OutsideWorld  &world,
                 const NodeId        &node_id,
                 const Slot          &initial_slot,
                 const Era           &initial_era,
                 const Configuration &initial_conf)
  : _world(world),
    _palladium(node_id, initial_slot, initial_era, initial_conf) {
}

std::ostream &Legislator::write_to(std::ostream &o) const {
  o << "-- palladium" << std::endl;
  o << _palladium;
  o << "-- timeout & roles:" << std::endl;
  o << "next_wake_up            = " <<
    std::chrono::time_point_cast<std::chrono::milliseconds>
      (_next_wake_up).time_since_epoch().count()
    << "ms" << std::endl;
  o << "retry_delay_ms          = " << _retry_delay_ms << "ms" << std::endl;

  o << "role                    = " << _role << " (";
  switch (_role) {
    case Role::candidate:
      o << "candidate";
      break;
    case Role::follower:
      o << "follower";
      break;
    case Role::leader:
      o << "leader";
      break;
    case Role::incumbent:
      o << "incumbent";
      break;
    default:
      o << "??";
      break;
  }
  o << ")" << std::endl;
  o << "leader                  = " << _leader_id << std::endl;

  o << "-- re-election:" << std::endl;
  if (_seeking_votes) {
    o << "offered_votes           =";
    for (auto const &n : _offered_votes) {
      o << " " << n;
    }
    o << std::endl;
  } else {
    o << "offered_votes           = not_seeking" << std::endl;
  }
  o << "minimum_term_for_peers  = " << _minimum_term_for_peers << std::endl;
  o << "attempted_term          = " << _attempted_term         << std::endl;
  o << "deferred_term           = " << _deferred_term          << std::endl;

  o << "-- RSM state:" << std::endl;
  o << "next_generated_node_id  = " << _next_generated_node_id << std::endl;
  o << "current_stream.owner    = " << _current_stream.owner   << std::endl;
  o << "current_stream.id       = " << _current_stream.id      << std::endl;
  o << "current_stream_pos      = " << _current_stream_pos     << std::endl;
  return o;
}

std::ostream& Paxos::operator<<(std::ostream &o, const Legislator &legislator) {
  return legislator.write_to(o);
}

std::ostream &Legislator::write_configuration_to(std::ostream &o) const {
  return o << "v" << _palladium.get_current_era()
           << ": " << _palladium.get_current_configuration()
           << std::endl;
}

std::chrono::steady_clock::duration Legislator::random_retry_delay() {
  if (_retry_delay_ms <= _minimum_retry_delay_ms) {
    return std::chrono::milliseconds(_minimum_retry_delay_ms);
  } else {
    return std::chrono::milliseconds(
         _minimum_retry_delay_ms + rand() % (_retry_delay_ms - _minimum_retry_delay_ms));
  }
}

void Legislator::handle_wake_up() {
  auto &now = _world.get_current_time();

  if (now < _next_wake_up) {
    return;
  }

#ifndef NTRACE
  std::cout << __PRETTY_FUNCTION__ << ": " <<
    std::chrono::time_point_cast<std::chrono::milliseconds>
      (now).time_since_epoch().count() << std::endl;
#endif // ndef NTRACE

  switch (_role) {
    case Role::candidate:
      _retry_delay_ms += _retry_delay_increment_ms;
      if (_retry_delay_ms > _maximum_retry_delay_ms) {
        _retry_delay_ms = _maximum_retry_delay_ms;
      }

      _offered_votes.clear();
      _seeking_votes = true;
      _world.seek_votes_or_catch_up
          (_palladium.next_chosen_slot(),
           _palladium.get_min_acceptable_term());

      handle_offer_vote(_palladium.node_id(),
                        _palladium.get_min_acceptable_term());

      set_next_wake_up_time(now + random_retry_delay());
      break;

    case Role::follower:
    case Role::incumbent:
      std::cout << "Leadership timed out, becoming candidate" << std::endl;
      _role = Role::candidate;
      _retry_delay_ms = _minimum_retry_delay_ms;
      set_next_wake_up_time(now + random_retry_delay());
      break;

    case Role::leader:
      _role = Role::incumbent;
      activate_slots(Value{.type = Value::Type::no_op}, 1);
      set_next_wake_up_time(now + _incumbent_timeout);
      break;
  }
}

void Legislator::handle_seek_votes_or_catch_up
      (const NodeId      &peer_id,
       const Slot        &slot,
       const Term        &term) {

  if (is_leading()) {
    if (_minimum_term_for_peers < term) {
      _minimum_term_for_peers = term;
    }
    if (_attempted_term < term) {
      start_term(_palladium.node_id());
    }
  }

  if (slot < _palladium.next_chosen_slot()) {
    _world.offer_catch_up(peer_id);
  } else if (slot == _palladium.next_chosen_slot()) {
    _world.offer_vote(peer_id, _palladium.get_min_acceptable_term());
  }
}

void Legislator::handle_offer_vote(const NodeId &peer_id,
                                   const Term   &min_acceptable_term) {
  if (_minimum_term_for_peers < min_acceptable_term) {
    _minimum_term_for_peers = min_acceptable_term;
  }

  if (_seeking_votes) {
    _offered_votes.insert(peer_id);
    if (_palladium.get_current_configuration().is_quorate(_offered_votes)) {
      _seeking_votes = false;
      _offered_votes.clear();
      start_term(_palladium.node_id());
    }
  } else {
    assert(_offered_votes.empty());
  }
}

void Legislator::handle_offer_catch_up(const NodeId &sender) {
  if (_seeking_votes) {
    _seeking_votes = false;
    _offered_votes.clear();
    _world.request_catch_up(sender);
  } else {
    assert(_offered_votes.empty());
  }
}

void Legislator::handle_request_catch_up(const NodeId &sender) {
  _world.send_catch_up(sender,
                       _palladium.next_chosen_slot(),
                       _palladium.get_current_era(),
                       _palladium.get_current_configuration(),
                       _next_generated_node_id,
                       _current_stream,
                       _current_stream_pos);
}

void Legislator::unsafely_stage_coup() {
  if (_role != Role::candidate) {
    return;
  }

  fprintf(stderr, "%s: WARNING risk of data loss\n", __PRETTY_FUNCTION__);

  Configuration conf(_palladium.node_id());

  handle_send_catch_up(_palladium.next_activated_slot() + 1,
                       _palladium.get_current_era() + 2,
                       conf,
                       _next_generated_node_id,
                       _current_stream,
                       _current_stream_pos);
}

void Legislator::handle_send_catch_up
   (const Slot          &slot,
    const Era           &era,
    const Configuration &conf,
    const NodeId        &next_generated_node,
    const Value::StreamName &current_stream,
    const uint64_t       current_stream_pos) {

  if (_palladium.next_chosen_slot() < slot) {
    _palladium.catch_up(slot, era, conf);

    assert(_next_generated_node_id <= next_generated_node);
    _next_generated_node_id = next_generated_node;

    if (current_stream.owner == _current_stream.owner
      && current_stream.id   == _current_stream.id) {
      assert(_current_stream_pos <= current_stream_pos);
    }

    _current_stream     = current_stream;
    _current_stream_pos = current_stream_pos;

    instant now = _world.get_current_time();
    if (_role != Role::candidate) {
      std::cout << __PRETTY_FUNCTION__ << ": becoming candidate" << std::endl;
      _role = Role::candidate;
    }
    set_next_wake_up_time(now + _follower_timeout);
  }
}

void Legislator::abdicate_to(const NodeId &node_id) {
  start_term(node_id);
}

void Legislator::start_term(const NodeId &owner_id) {
  if (_attempted_term.era < _palladium.get_current_era()) {
    _attempted_term.era         = _palladium.get_current_era();
    _attempted_term.term_number = 0;
    _attempted_term.owner       = owner_id;
  }

  if (_attempted_term < _minimum_term_for_peers) {
    _attempted_term = _minimum_term_for_peers;
  }

  const Term &minimum_term_for_self = _palladium.get_min_acceptable_term();
  if (_attempted_term < minimum_term_for_self) {
    _attempted_term = minimum_term_for_self;
  }

  if (owner_id < _attempted_term.owner) {
    _attempted_term.term_number += 1;
  }
  _attempted_term.owner = owner_id;

  _world.prepare_term(_attempted_term);
  handle_prepare_term(_palladium.node_id(), _attempted_term);
}

void Legislator::handle_prepare_term(const NodeId &sender, const Term &term) {
  if (_role == Role::follower && sender != _leader_id)           { return; }
  if (is_leading()            && sender != _palladium.node_id()) { return; }

  if (_palladium.get_current_era() < term.era) {
    _deferred_term = term;
  } else {
    auto promise = _palladium.handle_prepare(term);
    _world.record_promise(promise.term, promise.slots.start());
    if (promise.type == Promise::Type::multi
        || promise.slots.is_nonempty()) {
      if (term.owner == _palladium.node_id()) {
        handle_promise(_palladium.node_id(), promise);
      } else {
        _world.make_promise(promise);
      }
    }
  }
}

void Legislator::handle_promise(const NodeId &sender, const Promise &promise) {
  handle_proposal(_palladium.handle_promise(sender, promise), true);

  if (!_palladium.has_active_slots()) {
    handle_proposal(_palladium.activate({.type = Value::Type::no_op}, 1), true);
  }
}

