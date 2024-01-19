# Refactor Retention Test Cases

## Code Details

Build the `RefactorRetentionCommand.scala`` with the `core:jar`` gradle task


**Legend**

| Milliseconds | per ...    |
| ------------ | ---------- |
| 1000         | second     |
| 60000        | minute     |
| 3600000      | hour       |
| 43200000     | half day   |
| 86400000     | One day    |
| 172800000    | Two days   |
| 259200000    | Three days |
| 604800000    | Seven days |


## Data for tests (unless included in the test, use these for populating topics)

messages-old-and-new
```
0,-259200000,.,3 days old
0,-172800000,.,2 days old
0,-86400000,.,1 days old
0,-3600000,.,1 hour old
0,-5,.,5 ms ago
0,-4,.,5 ms ago
0,-3,.,5 ms ago
0,-2,.,5 ms ago
0,-1,.,5 ms ago
1,-259200000,.,3 days old
1,-172800000,.,2 days old
1,-86400000,.,1 days old
1,-3600000,.,1 hour old
1,-5,.,5 ms ago
1,-4,.,5 ms ago
1,-3,.,5 ms ago
1,-2,.,5 ms ago
1,-1,.,5 ms ago
2,-259200000,.,3 days old
2,-172800000,.,2 days old
2,-86400000,.,1 days old
2,-3600000,.,1 hour old
2,-5,.,5 ms ago
2,-4,.,5 ms ago
2,-3,.,5 ms ago
2,-2,.,5 ms ago
2,-1,.,5 ms ago
```

messages-one-per-day
```
-604800000,.,7 days old
-518400000,.,6 days old
-432000000,.,5 days old
-345600000,.,4 days old
-259200000,.,3 days old
-172800000,.,2 days old
-86400000,.,1 days old
-1,.,1 ms ago
```

## TEST: No Consumer Group Found

### Setup
None.

### Test

```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group foo.notFound --retention-min 60000
```

Result

```
No Groups found for [foo.notFound]
```

### Cleanup
None.

## TEST: Three Topics with No Overlapping Consumer Groups

| Topic | Partitions | Retention |
| ----- | ---------- | --------- |
| foo.a | 3          | 604800000 |
| foo.b | 3          | 604800000 |
| foo.c | 3          | 604800000 |

#### Topic
```
bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --create --replication-factor 1 \
 --partitions 3 --config retention.ms=604800000 \
 --topic foo.a

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --create --replication-factor 1 \
 --partitions 3 --config retention.ms=604800000 \
 --topic foo.b

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --create --replication-factor 1 \
 --partitions 3 --config retention.ms=604800000 \
 --topic foo.c

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --create --replication-factor 1 \
 --partitions 100 --config retention.ms=604800000 \
 --topic foo.large
```

#### Data

Deploy `messages-old-and-new` Messages:
```
cat messages-old-and-new | bin/kafka-console-producer.sh \
  --bootstrap-server ${BROKER}:9095 \
  --property key.separator=, --property null.marker=. \
  --property parse.key=true --property parse.partition=true \
  --property parse.timestamp=true \
  --topic foo.a

cat messages-old-and-new | bin/kafka-console-producer.sh \
  --bootstrap-server ${BROKER}:9095 \
  --property key.separator=, --property null.marker=. \
  --property parse.key=true --property parse.partition=true \
  --property parse.timestamp=true \
  --topic foo.b

cat messages-old-and-new | bin/kafka-console-producer.sh \
  --bootstrap-server ${BROKER}:9095 \
  --property key.separator=, --property null.marker=. \
  --property parse.key=true --property parse.partition=true \
  --property parse.timestamp=true \
  --topic foo.c

cat messages-old-and-new | bin/kafka-console-producer.sh \
  --bootstrap-server ${BROKER}:9095 \
  --property key.separator=, --property null.marker=. \
  --property parse.key=true --property parse.partition=true \
  --property parse.timestamp=true \
  --topic foo.large
```

#### Consumers

* `con.a` will be at latest
* `con.b` will be one hour old on partition 2
* `con.c` will be one day behind on all
* Expected result will be one day setting (86400000) for the topic

```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.a \
  --topic foo.a:0,1,2 \
  --to-latest

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.b \
  --topic foo.b:0,1 \
  --to-latest

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.b \
  --topic foo.b:2 \
  --to-offset 3

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.c \
  --topic foo.c:0,1,2 \
  --to-offset 2

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.x \
  --topic foo.c:0,2 \
  --to-offset 2

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.large \
  --topic foo.large:0,2 \
  --to-offset 2
```

#### Validate Test Setup

```
bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.a

bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.b

bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.c

bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.x

bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.large
```

Result, shortened for brevity
```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
con.a           foo.a           0          9               9               0               -               -               -
con.a           foo.a           1          9               9               0               -               -               -
con.a           foo.a           2          9               9               0               -               -               -
con.b           foo.b           0          9               9               0               -               -               -
con.b           foo.b           1          9               9               0               -               -               -
con.b           foo.b           2          3               9               6               -               -               -
con.c           foo.c           0          2               9               7               -               -               -
con.c           foo.c           1          2               9               7               -               -               -
con.c           foo.c           2          2               9               7               -               -               -
con.x           foo.c           0          2               9               7               -               -               -
con.x           foo.c           2          2               9               7               -               -               -
```
NOTE: `con.x` is missing Partion 1

### Tests

1. Calculate retention suggestion given a min retention of 2 days
```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group con.* --retention-min 172800000
```

Result
```
WARN: No action will be performed as the --execute option is missing.

TOPIC                                                        RETENTION        NEW-RETENTION
foo.a                                                        604800000        172800000
foo.c                                                        604800000        176400000
foo.b                                                        604800000        172800000
```

2. Calculate retention suggestion given a min retention of 1 minute
```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group con.* --retention-min 60000
```

Result
```
WARN: No action will be performed as the --execute option is missing.

TOPIC                                                        RETENTION        NEW-RETENTION
foo.a                                                        604800000        3660000
foo.c                                                        604800000        176460000
foo.b                                                        604800000        90060000
```

### Cleanup
```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --delete-offsets --execute \
  --topic foo.a \
  --group con.a

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --delete-offsets --execute \
  --topic foo.b \
  --group con.b

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --delete-offsets --execute \
  --topic foo.c \
  --group con.c

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --delete \
 --topic foo.a

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --delete \
 --topic foo.b

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --delete \
 --topic foo.c

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --delete \
 --topic foo.large
```

## TEST: Only One Topic Matches with Multiple Consumer Groups

### Setup

| Topic | Partitions | Retention |
| ----- | ---------- | --------- |
| foo.a | 3          | 604800000 |

#### Topic
```
bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --create --replication-factor 1 \
 --partitions 3 --config retention.ms=604800000 \
 --topic foo.a
```

#### Data

Deploy `messages-old-and-new` Messages:
```
cat messages-old-and-new | bin/kafka-console-producer.sh \
  --bootstrap-server ${BROKER}:9095 \
  --property key.separator=, --property null.marker=. \
  --property parse.key=true --property parse.partition=true \
  --property parse.timestamp=true \
  --topic foo.a
```

#### Consumers

* `con.a` will be at latest
* `con.b` will be one hour old on partition 2
* `con.c` will be one day behind on all
* Expected result will be one day setting (86400000) for the topic

con.a
```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.a \
  --topic foo.a:0,1,2 \
  --to-latest
```

con.b
```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.b \
  --topic foo.a:0,1 \
  --to-latest

bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.b \
  --topic foo.a:2 \
  --to-offset 3
```

con.c
```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.c \
  --topic foo.a:0,1,2 \
  --to-offset 2
```

#### Validate Test Setup

```
bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.a

bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.b

bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.c
```

Result, shortened for brevity
```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
con.a           foo.a           0          9               9               0               -               -               -
con.a           foo.a           1          9               9               0               -               -               -
con.a           foo.a           2          9               9               0               -               -               -
con.b           foo.a           0          9               9               0               -               -               -
con.b           foo.a           1          9               9               0               -               -               -
con.b           foo.a           2          3               9               6               -               -               -
con.c           foo.a           0          2               9               7               -               -               -
con.c           foo.a           1          2               9               7               -               -               -
con.c           foo.a           2          2               9               7               -               -               -
```

### Tests

1. Calculate retention suggestion given a min retention of 2 days
```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group con.* --retention-min 172800000
```

Result
```
WARN: No action will be performed as the --execute option is missing.

TOPIC                                                        RETENTION        NEW-RETENTION
foo.a                                                        604800000        172800000
```

2. Calculate retention suggestion given a min retention of 1 minute
```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group con.* --retention-min 60000
```

Result
```
WARN: No action will be performed as the --execute option is missing.

TOPIC                                                        RETENTION        NEW-RETENTION
foo.a                                                        604800000        90060000
```

Since the oldest consumer is 1 day behind, and the default step is 1 hour, if we add all these
numbers up, we get the matching new-retention suggestion
```
90060000 => 1 days, 1 hours, 1 minutes and 0 seconds.
```

3. Calculate and Set the minimum retention
```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group con.* --retention-min 60000 --execute
```

Result
```
TOPIC                                                        RETENTION        NEW-RETENTION
foo.a                                                        604800000        90060000
Setting retention for foo.a to 90060000.
```

4. Validate the topic retention was changed.
```
bin/kafka-topics.sh  --bootstrap-server ${BROKER}:9095  --topic foo.a --describe
```

Result
```
Topic: foo.a    TopicId: aXueQoR9Sv2P6dxflks-pQ PartitionCount: 3       ReplicationFactor: 1    Configs: segment.bytes=1073741824,retention.ms=90060000,max.message.bytes=3000000
        Topic: foo.a    Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: foo.a    Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: foo.a    Partition: 2    Leader: 1       Replicas: 1     Isr: 1
```


### Cleanup
```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --delete-offsets --execute \
  --topic foo.a
  --all-groups

bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --delete \
 --topic foo.a
```

## TEST: One Topic with One Consumer Group, but missing offset commits on one partition

| Topic | Partitions | Retention |
| ----- | ---------- | --------- |
| foo.a | 3          | 604800000 |

#### Topic
```
bin/kafka-topics.sh \
 --bootstrap-server ${BROKER}:9095 \
 --create --replication-factor 1 \
 --partitions 3 --config retention.ms=604800000 \
 --topic foo.a
```

#### Data

Deploy `messages-old-and-new` Messages:
```
cat messages-old-and-new | bin/kafka-console-producer.sh \
  --bootstrap-server ${BROKER}:9095 \
  --property key.separator=, --property null.marker=. \
  --property parse.key=true --property parse.partition=true \
  --property parse.timestamp=true \
  --topic foo.a
```

#### Consumers

* `con.a` will be at latest except on partition 1
* Expected result will be one day setting (86400000) for the topic

```
bin/kafka-consumer-groups.sh \
  --bootstrap-server ${BROKER}:9095 \
  --reset-offsets --execute \
  --group con.a \
  --topic foo.a:0,2 \
  --to-latest
```

#### Validate Test Setup
```
bin/kafka-consumer-groups.sh  --bootstrap-server ${BROKER}:9095 \
  --describe --group con.a
```

Result, notice partition 1 is missing
```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
con.a           foo.a           0          9               9               0               -               -               -
con.a           foo.a           2          9               9               0               -               -               -
```

### Tests

1. Calculate retention suggestion given a min retention of 1 minute
```
bin/kafka-refactor-retention.sh --bootstrap-server ${BROKER}:9095 --group con.* --retention-min 60000
```

## TEST: Three Topics Match with Overlapping Multiple Consumer Groups (3T3CG)

## TEST: 3T3CG with One Very Laggy Partition

## TEST: Lots of Partitions
