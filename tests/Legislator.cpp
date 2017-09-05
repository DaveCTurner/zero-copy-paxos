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

Configuration create_conf();

class TracingOutsideWorld : public OutsideWorld {
  const instant start_time;
        instant current_time;
        instant next_wake_up_time;

public:
  TracingOutsideWorld(instant current_time)
    : start_time(current_time),
      current_time(current_time),
      next_wake_up_time(current_time)
    { }

  const instant get_current_time() override {
    return current_time;
  }

  void set_next_wake_up_time(const instant &t) override {
    next_wake_up_time = t;
    std::cout << "RESPONSE: set_next_wake_up_time(" <<
      std::chrono::duration_cast<std::chrono::milliseconds>
        (t-start_time).count()
      << "ms)" << std::endl;
  }

  void tick() {
    current_time = next_wake_up_time;
    std::cout << "current_time(" <<
      std::chrono::duration_cast<std::chrono::milliseconds>
        (current_time-start_time).count()
      << "ms)" << std::endl;
  }

  void seek_votes_or_catch_up(const Slot &slot, const Term &term) override {
    std::cout << "RESPONSE: seek_votes_or_catch_up("
      << slot << ", "
      << term << ")" << std::endl;
  }

  void offer_vote(const NodeId &recipient, const Term &term) override {
    std::cout << "RESPONSE: offer_vote("
      << recipient << ", "
      << term << ")" << std::endl;
  }

  void offer_catch_up(const NodeId &recipient) override {
    std::cout << "RESPONSE: offer_catch_up("
      << recipient << ")" << std::endl;
  }

  void request_catch_up(const NodeId &recipient) override {
    std::cout << "RESPONSE: request_catch_up("
      << recipient << ")" << std::endl;
  }

  void prepare_term(const Term &term) override {
    std::cout << "RESPONSE: prepare_term("
      << term << ")" << std::endl;
  }

  void record_promise(const Term &term, const Slot &slot) override {
    std::cout << "RESPONSE: record_promise("
      << term << "," << slot << ")" << std::endl;
  }

  void make_promise(const Promise &promise) override {
    std::cout << "RESPONSE: make_promise("
      << promise << ")" << std::endl;
  }

  void proposed_and_accepted(const Proposal &proposal) override {
    std::cout << "RESPONSE: proposed_and_accepted("
      << proposal << ")" << std::endl;
  }
};

void legislator_test() {
  uint32_t seed = rand();
  std::cout << std::endl << "legislator_test(): seed = " << seed << std::endl;
  srand(seed);

  auto conf = create_conf();
  TracingOutsideWorld world(std::chrono::steady_clock::now());
  Legislator legislator(world, 1, 0, 0, conf);

  std::cout << std::endl << "TEST: Initial state" << std::endl;
  std::cout << legislator << std::endl;
  world.tick();

  std::cout << std::endl << "TEST: handle_wake_up()" << std::endl;
  legislator.handle_wake_up();
  std::cout << legislator << std::endl;

  std::cout << std::endl << "TEST: handle_offer_catch_up(3)" << std::endl;
  legislator.handle_offer_catch_up(3);
  std::cout << legislator << std::endl;

  world.tick();
  std::cout << std::endl << "TEST: handle_wake_up()" << std::endl;
  legislator.handle_wake_up();
  std::cout << legislator << std::endl;

  std::cout << std::endl << "TEST: handle_offer_vote(2,[0.3.2])" << std::endl;
  legislator.handle_offer_vote(2, Term(0,3,2));
  std::cout << legislator << std::endl;

  std::cout << std::endl << "TEST: handle_seek_votes_or_catch_up(2, 0, [0.4.2])" << std::endl;
  legislator.handle_seek_votes_or_catch_up(2, 0, Term(0,4,2));

  std::cout << std::endl << "TEST: handle_prepare_term(2,[0.5.2])" << std::endl;
  legislator.handle_prepare_term(2, Term(0,5,2));
  std::cout << legislator << std::endl;

  world.tick();
  std::cout << std::endl << "TEST: handle_wake_up()" << std::endl;
  legislator.handle_wake_up();
  std::cout << std::endl << "TEST: handle_offer_vote(2,[0.5.2])" << std::endl;
  legislator.handle_offer_vote(2, Term(0,5,2));

  auto promise = Promise(Promise::Type::multi, 0, 0, Term(0,6,1));
  std::cout << std::endl << "TEST: handle_promise(2," << promise << ")" << std::endl;
  legislator.handle_promise(2, promise);
  std::cout << legislator << std::endl;

  auto prop = Proposal({
    .slots = SlotRange(0,1),
    .term  = Term(0,6,1),
    .value = {.type = Value::Type::no_op }});
  std::cout << std::endl << "TEST: handle_accepted(3," << prop << ")" << std::endl;
  legislator.handle_accepted(3, prop);
  std::cout << legislator << std::endl;

  std::cout << std::endl << "TEST: handle_seek_votes_or_catch_up(2, 0, [0.7.2])" << std::endl;
  legislator.handle_seek_votes_or_catch_up(2, 0, Term(0,7,2));
  std::cout << legislator << std::endl;
}
