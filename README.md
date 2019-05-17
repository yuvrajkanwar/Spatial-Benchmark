
Credit Links
-----
http://wiki.github.com/brianfrankcooper/YCSB/  
https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/
ycsb-users@yahoogroups.com  

Building from source
--------------------

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl com.yahoo.ycsb:mongodb-binding -am clean package

Executing Benchmark
--------------------

1. MongoDB

./bin/ycsb load mongodb -P workloads/geo/workloadga -p mongodb.url="mongodb://localhost:27017/grafittiDB?w=1" -p mongodb.auth="true"

./bin/ycsb run mongodb -P workloads/geo/workloadga -p mongodb.url="mongodb://localhost:27017/grafittiDB?w=1" -p mongodb.auth="true"

2. Couchbase:

./bin/ycsb load couchbase2 -P workloads/geo/workloadga -p couchbase.host="localhost:8091"
./bin/ycsb run couchbase2 -P workloads/geo/workloadga -p couchbase.host="localhost:8091"
    
Yahoo! Cloud System Benchmark (YCSB)
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)
