ORC TEST BED
============

This is just an ORC writer in a loop test for now.

To build, you need to have a local build of hive-apache trunk and then just run `mvn package` as the same user.

To run the test, do `hadoop jar target/orcery-1.0-SNAPSHOT.jar -o /tmp/orc.$RANDOM/ -n 500`.
