How to run the cleanup-framework
================================

Use the Marathon UI to deploy the docker image:

  arangodb/arangodb-cleanup-framework

with the following command:

  ./arangodb-cleanup-framework --master=zk://master.mesos:2181/mesos --zk=zk://master.mesos:2181

Make sure all running tasks of the arangodb framework are killed.
Then the zookeeper state and all reservations and persistent volumes
will be cleaned up within minutes.

Then simply destroy the Marathon app for the cleanup-framework.

Alternatively, use curl to start the cleanup-framework by doing

    curl -X POST -H "Content-Type: application/json" http://<your-mesos-master-url>:8080/v2/apps -d @cleanup.json --dump - && echo

where `cleanup.json` looks like the following:


    {
      "id": "cleanup",
      "cpus": 1,
      "mem": 512.0,
      "ports": [],
      "instances": 1,
      "args": [
        "/arangodb-cleanup-framework",
        "--name=arangodb3",
        "--master=zk://master.mesos:2181/mesos",
        "--zk=zk://master.mesos:2181/arangodb3",
        "--principal=arangodb3",
        "--role=arangodb3"
      ],
      "env": {
      },
      "container": {
        "type": "DOCKER",
        "docker": {
          "image": "arangodb/arangodb-cleanup-framework",
          "forcePullImage": true,
          "network": "HOST"
        }
      }
    }


