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



#include "Command/Registration.h"
#include "Command/Listener.h"
#include "Pipeline/Peer/Target.h"
#include "RealWorld.h"
#include "Pipeline/Client/Listener.h"
#include "Pipeline/Peer/Listener.h"
#include "Epoll.h"
#include "Paxos/Legislator.h"

#include <getopt.h>
#include <signal.h>

struct option long_options[] =
  {
    {"client-port",   required_argument, 0, 'c'},
    {"peer-port",     required_argument, 0, 'p'},
    {"command-port",  required_argument, 0, 'm'},
    {"target",        required_argument, 0, 't'},
    {"register-at",   required_argument, 0, 'r'},
    {0, 0, 0, 0}
  };

int main(int argc, char **argv) {
  const char *client_port   = NULL;
  const char *peer_port     = NULL;
  const char *command_port  = NULL;
  std::vector<Pipeline::Peer::Target::Address> target_addresses;
  std::vector<Command::Registration::Address>  registration_addresses;

  while (1) {
    int option_index = 0;
    int getopt_result = getopt_long(argc, argv, "c:p:m:t:r:",
                                    long_options, &option_index);

    if (getopt_result == -1) { break; }

    char *target, *p;

    switch (getopt_result) {
      case 'c':
        if (client_port != NULL) {
          fprintf(stderr, "--client-port repeated\n");
          abort();
        }
        client_port = strdup(optarg);
        if (client_port == NULL) {
          perror("getopt: client_port");
          abort();
        }
        break;
      case 'p':
        if (peer_port != NULL) {
          fprintf(stderr, "--peer-port repeated\n");
          abort();
        }
        peer_port = strdup(optarg);
        if (peer_port == NULL) {
          perror("getopt: peer_port");
          abort();
        }
        break;
      case 'm':
        if (command_port != NULL) {
          fprintf(stderr, "--command-port repeated\n");
          abort();
        }
        command_port = strdup(optarg);
        if (command_port == NULL) {
          perror("getopt: command_port");
          abort();
        }
        break;
      case 't':
        target = strdup(optarg);
        if (target == NULL) {
          perror("getopt: target");
          abort();
        }
        p = target;
        while (*p) {
          if (*p == ':') {
            *p = '\0';
            target_addresses.push_back(
              Pipeline::Peer::Target::Address(target, p+1));
            break;
          }
          p++;
        }
        free(target);
        break;

      case 'r':
        target = strdup(optarg);
        if (target == NULL) {
          perror("getopt: register_at");
          abort();
        }
        p = target;
        while (*p) {
          if (*p == ':') {
            *p = '\0';
            registration_addresses.push_back(
              Command::Registration::Address(target, p+1));
            break;
          }
          p++;
        }
        free(target);
        break;

      default:
        fprintf(stderr, "unknown option\n");
        abort();
    }
  }

  if (client_port == NULL) {
    fprintf(stderr, "option --client-port is required\n");
    abort();
  }

  if (peer_port == NULL) {
    fprintf(stderr, "option --peer-port is required\n");
    abort();
  }

  if (command_port == NULL) {
    fprintf(stderr, "option --command-port is required\n");
    abort();
  }

  std::string cluster_name;
  Paxos::NodeId node_id;
  Command::Registration::get_node_name(cluster_name, node_id,
                                       registration_addresses);
  const Pipeline::NodeName node_name(cluster_name, node_id);

  printf("Starting as cluster %s node %d\n", node_name.cluster.c_str(),
                                             node_name.id);

  std::cout << "Targets:" << std::endl;
  for (const auto &address : target_addresses) {
    std::cout << address.host << " port " << address.port << std::endl;
  }

  Paxos::Configuration conf(1);
  RealWorld real_world(node_name);
  Paxos::Legislator legislator(real_world, node_name.id, 0, 0, conf);
  Epoll::Manager manager;
  Pipeline::Client::Listener client_listener
    (manager, legislator, node_name, client_port);
  Pipeline::Peer::Listener peer_listener
    (manager, legislator, node_name, peer_port);
  Command::Listener command_listener
    (manager, legislator, node_name, command_port);

  real_world.add_chosen_value_handler(&client_listener);
  real_world.set_node_id_generation_handler(&command_listener);

  std::vector<std::unique_ptr<Pipeline::Peer::Target>> targets;
  for (const auto &address : target_addresses) {
    targets.push_back(std::move(std::unique_ptr<Pipeline::Peer::Target>
      (new Pipeline::Peer::Target(address, manager, legislator, node_name))));
  }

  const std::chrono::steady_clock::duration
                                    target_check_interval
          = std::chrono::milliseconds(500);

  std::chrono::steady_clock::time_point
                                    next_target_check_time
                                      = real_world.get_current_time()
                                      + target_check_interval;

  signal(SIGPIPE, SIG_IGN);

  while (1) {
    auto ms_to_next_wake_up
      = std::chrono::duration_cast<std::chrono::milliseconds>
          (real_world.get_next_wake_up_time()
            - real_world.get_current_time()).count();

    if (ms_to_next_wake_up < 0) {
      ms_to_next_wake_up = 0;
    }

    manager.wait(ms_to_next_wake_up);

    legislator.handle_wake_up();

    if (next_target_check_time < real_world.get_current_time()) {
      for (auto &target : targets) {
        target->start_connection();
      }

      next_target_check_time = real_world.get_current_time()
                             + target_check_interval;
    }
  }

  return 1;
}
