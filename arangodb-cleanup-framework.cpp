// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>

#include <sstream>
#include <vector>
#include <regex>

#include <glog/logging.h>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include <mesos/type_utils.hpp>

#include <mesos/authorizer/authorizer.hpp>

#include <stout/flags.hpp>
#include <stout/format.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/protobuf.hpp>
#include <stout/stringify.hpp>
#include <stout/uuid.hpp>

#include "common/status_utils.hpp"

#include "logging/flags.hpp"
#include "logging/logging.hpp"

#include "state/zookeeper.hpp"
#include "state/state.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::state;

using std::cerr;
using std::cout;
using std::endl;
using std::ostringstream;
using std::string;
using std::vector;

// The framework launches a task on each registered slave using a
// persistent volume. It restarts the task once the previous one on
// the slave finishes. The framework terminates once the number of
// tasks launched on each slave reaches a limit.
class CleanupScheduler : public Scheduler
{
public:
  CleanupScheduler(
      const FrameworkInfo& _frameworkInfo,
      const std::string &_zk)
    : frameworkInfo(_frameworkInfo), zk(_zk)
  {
  }

  void cleanupZookeeper() {
    string userAndPass  = "(([^/@:]+):([^/@]*)@)";
    string hostAndPort  = "[A-z0-9\\.-]+(:[0-9]+)?";
    string hostAndPorts = "(" + hostAndPort + "(," + hostAndPort + ")*)";
    string zkNode       = "[^/]+(/[^/]+)*";
    string REGEX        = "zk://(" + userAndPass +"?" + hostAndPorts + ")(/" + zkNode + ")";

    std::regex re(REGEX);
    std::smatch m;
    bool ok = regex_match(zk, m, re);

    if (! ok) {
      LOG(ERROR) << "FATAL cannot parse zookeeper '" << zk << "'";
      exit(EXIT_FAILURE);
    }

    string prefix(m[9]);
    string name = prefix.substr(1);
    ZooKeeperStorage storage = ZooKeeperStorage(m[1], Seconds(120), prefix);
    State state = State(&storage);

    Variable variable = state.fetch("state_" + name).get();
    
    LOG(INFO) << "Cleaning up Zookeeper state at " << zk << "(" << name << ")";
    auto r = state.expunge(variable);
    r.await();  // Wait until state is actually expunged

    LOG(INFO) << "Zookeeper cleanup result: " << r.get();
  }

  virtual void registered(
      SchedulerDriver* driver,
      const FrameworkID& frameworkId,
      const MasterInfo& masterInfo)
  {
    LOG(INFO) << "Registered with master " << masterInfo
              << " and got framework ID " << frameworkId;

    frameworkInfo.mutable_id()->CopyFrom(frameworkId);

    cleanupZookeeper();
  }

  virtual void reregistered(
      SchedulerDriver* driver,
      const MasterInfo& masterInfo)
  {
    LOG(INFO) << "Reregistered with master " << masterInfo;
  }

  virtual void disconnected(
      SchedulerDriver* driver)
  {
    LOG(INFO) << "Disconnected!";
  }

  virtual void resourceOffers(
      SchedulerDriver* driver,
      const vector<Offer>& offers)
  {
    foreach (const Offer& offer, offers) {
      LOG(INFO) << "Received offer " << offer.id() << " from slave "
                << offer.slave_id() << " (" << offer.hostname() << ") "
                << "with " << offer.resources();
      
      Resources offered = offer.resources();
      Resources persistentVolumes = offered.persistentVolumes();

      vector<Offer::Operation> operations = {};
      if (!persistentVolumes.empty()) {
        mesos::Offer::Operation destroy;
        destroy.set_type(mesos::Offer::Operation::DESTROY);
        destroy.mutable_destroy()->mutable_volumes()->CopyFrom(persistentVolumes);
        
        operations.push_back(destroy);
        LOG(INFO) << "Deleting volumes " << persistentVolumes << " from " << offer.id();
      }
      
      Resources reserved = offered.reserved(frameworkInfo.role());

      if (!reserved.empty()) {
        mesos::Offer::Operation unreserve;
        unreserve.set_type(mesos::Offer::Operation::UNRESERVE);
        unreserve.mutable_unreserve()->mutable_resources()->CopyFrom(reserved);
        
        operations.push_back(unreserve);
        LOG(INFO) << "Unreserving " << reserved << " from " << offer.id();
      }
      
      if (!operations.empty()) {
        driver->acceptOffers({offer.id()}, operations);
      }
    }
  }

  virtual void offerRescinded(
      SchedulerDriver* driver,
      const OfferID& offerId)
  {
    LOG(INFO) << "Offer " << offerId << " has been rescinded";
  }

  virtual void statusUpdate(
      SchedulerDriver* driver,
      const TaskStatus& status)
  {
    LOG(INFO) << "Task '" << status.task_id() << "' is in state "
              << status.state();
  }

  virtual void frameworkMessage(
      SchedulerDriver* driver,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      const string& data)
  {
    LOG(INFO) << "Received framework message from executor '" << executorId
              << "' on slave " << slaveId << ": '" << data << "'";
  }

  virtual void slaveLost(
      SchedulerDriver* driver,
      const SlaveID& slaveId)
  {
    LOG(INFO) << "Lost slave " << slaveId;
  }

  virtual void executorLost(
      SchedulerDriver* driver,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      int status)
  {
    LOG(INFO) << "Lost executor '" << executorId << "' on slave "
              << slaveId << ", " << WSTRINGIFY(status);
  }

  virtual void error(
      SchedulerDriver* driver,
      const string& message)
  {
    LOG(ERROR) << message;
  }

private:
  FrameworkInfo frameworkInfo;
  const std::string &zk;
};


class Flags : public logging::Flags
{
public:
  Flags()
  {
    add(&master,
        "master",
        "The master to connect to. May be one of:\n"
        "  master@addr:port (The PID of the master)\n"
        "  zk://host1:port1,host2:port2,.../path\n"
        "  zk://username:password@host1:port1,host2:port2,.../path\n"
        "  file://path/to/file (where file contains one of the above)");
    
    add(&zk,
        "zk",
        "zookeeper url of the arangodb to clean up");

    add(&role,
        "role",
        "Role to use when registering",
        "arangodb");

    add(&principal,
        "principal",
        "The principal used to identify this framework",
        "arangodb");
  }

  Option<string> master;
  Option<string> zk;
  string role;
  string principal;
};


int main(int argc, char** argv)
{
  Flags flags;

  Try<Nothing> load = flags.load("MESOS_", argc, argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.master.isNone()) {
    cerr << flags.usage("Missing required option --master") << endl;
    return EXIT_FAILURE;
  }

  if (flags.zk.isNone()) {
    cerr << flags.usage("Missing required option --zk") << endl;
    return EXIT_FAILURE;
  }

  logging::initialize(argv[0], flags, true); // Catch signals.

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name("ArangoDB Cleanup Framework");
  framework.set_role(flags.role);
  framework.set_checkpoint(true);
  framework.set_principal(flags.principal);

  CleanupScheduler scheduler(
      framework,
      flags.zk.get()
  );

  MesosSchedulerDriver* driver = new MesosSchedulerDriver(
      &scheduler,
      framework,
      flags.master.get());

  int status = driver->run() == DRIVER_STOPPED ? EXIT_SUCCESS : EXIT_FAILURE;

  driver->stop();
  delete driver;
  return status;
}
