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



#include "Paxos/Palladium.h"

#include <deque>
#include <memory>

using namespace Paxos;

Configuration create_conf();

enum message_type_t {
  prepare,
  promised,
  activate,
  proposed,
  accepted,
  chosen
};

struct message {
  message_type_t type;
  NodeId         sender;
  Term           prepare;
  Promise        promise = Promise(Promise::Type::none, 0, 0, Term(0,0,0));
  Proposal       proposal = {.slots = SlotRange(0,0)};
  uint64_t       activate_count;
};

void process_message(const message &m, Palladium &n,
    std::deque<message> &q, bool &made_progress) {

    message r;
    r.sender = n.node_id();
    switch(m.type) {
      case message_type_t::prepare:
        r.type = message_type_t::promised;
        r.promise = n.handle_prepare(m.prepare);
        if (r.promise.type != Promise::Type::none
            && (r.promise.type == Promise::Type::multi
              || r.promise.slots.is_nonempty())) {
          q.push_back(r);
        }
        break;
      case message_type_t::promised:
        r.type = message_type_t::proposed;
        r.proposal = n.handle_promise(m.sender, m.promise);
        if (r.proposal.slots.is_nonempty()) {
          q.push_back(r);
        }
        break;
      case message_type_t::activate:
        r.type     = message_type_t::proposed;
        r.proposal = n.activate(m.proposal.value, m.activate_count);
        if (r.proposal.slots.is_nonempty()) {
          q.push_back(r);
        }
      case message_type_t::proposed:
        if (n.handle_proposal(m.proposal)) {
          r.type = message_type_t::accepted;
          r.proposal = m.proposal;
          q.push_back(r);
        }
        break;
      case message_type_t::accepted:
        n.handle_accepted(m.sender, m.proposal);

        r.proposal = n.check_for_chosen_slots();
        while (r.proposal.slots.is_nonempty()) {
          made_progress = true;
          r.type = message_type_t::chosen;
          q.push_back(r);
          r.proposal = n.check_for_chosen_slots();
        }
        break;
      case message_type_t::chosen:
        break;
    }
}

void palladium_random_safety_test() {
  auto conf = create_conf();
  std::vector<std::unique_ptr<Palladium>> nodes;
  for (int i = 1; i <= 4; i++) {
    nodes.push_back(std::move(std::unique_ptr<Palladium>(new Palladium(i, 0, 0, conf))));
  }

  uint32_t seed = rand();
  std::cout << "seed = " << seed << std::endl;
  srand(seed);

  std::deque<message> messages;
  for (uint64_t iteration = 0; iteration < 50000; iteration++) {
    auto message_index = rand() % (messages.size() + 1);
    if (message_index == messages.size()) {
      message m;
      switch (rand() % 2) {
        case 0:
          m.sender  = 0;
          m.type    = message_type_t::prepare;
          m.prepare = Term(0, rand() % (1+(iteration / 10000)),
                                1 + (rand() % nodes.size()));
          break;
        case 1:
          m.sender   = 0;
          m.type     = message_type_t::activate;
          Value &v   = m.proposal.value;
          v.type     = Value::Type::stream_content;
          v.payload.stream.name.owner = 1+(rand() % 4);
          v.payload.stream.name.id    = rand();
          v.payload.stream.offset     = rand();
          m.activate_count = rand() % 50;
          break;
      }
      messages.push_back(m);
    }

    const message &m = messages[message_index];
    auto &n = nodes[rand() % nodes.size()];
    bool made_progress = false;
    process_message(m, *n, messages, made_progress);
  }

  while (!messages.empty()) {
    const message m = messages.front();
    messages.pop_front();
    for (auto &n : nodes) {
      bool made_progress = false;
      if (rand() % 2 < 1) {
        process_message(m, *n, messages, made_progress);
      }
    }
  }

  message final_prep;
  final_prep.type = message_type_t::prepare;
  final_prep.sender = 0;
  for (const auto &n : nodes) {
    if (final_prep.prepare < n->next_activated_term()) {
      final_prep.prepare = n->next_activated_term();
    }
    if (final_prep.prepare < n->get_min_acceptable_term()) {
      final_prep.prepare = n->get_min_acceptable_term();
    }
  }
  final_prep.prepare.term_number += 1;

  bool made_progress = true;
  while (made_progress) {
    made_progress = false;

    messages.push_back(final_prep);

    while (!messages.empty()) {
      const message m = messages.front();
      messages.pop_front();
      for (auto &n : nodes) {
        process_message(m, *n, messages, made_progress);
      }
    }
  }
}
