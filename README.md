How to run the cleanup-framework
================================

Use the Marathon UI to deploy the docker image:

  m0ppers/arangodb-cleanup-framework

with the following command:

  /arangodb-cleanup-framework --master=zk://master.mesos:2181/mesos --zk=zk://master.mesos:2181

Make sure all running tasks of the arangodb framework are killed.
Then the zookeeper state and all reservations and persistent volumes
will be cleaned up within minutes.

Then simply destroy the Marathon app for the cleanup-framework.
