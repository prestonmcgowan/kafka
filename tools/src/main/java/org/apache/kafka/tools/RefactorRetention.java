/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This tool helps to refactor identified consumer groups based on consumption.
 * It analyzes consumer group consumption patterns and recommends retention time adjustments
 * for topics based on the oldest consumed messages.
 */
public class RefactorRetention {

    private static final String DEFAULT_CONFLUENT_GROUP_PREFIX = "^_confluent-.*";
    private static final long DEFAULT_RETENTION = 345600000L; // 4 days
    private static final long DEFAULT_RETENTION_MIN = 3600000L; // 1 hour
    private static final long DEFAULT_RETENTION_MAX = 604800000L; // 7 days
    private static final long DEFAULT_RETENTION_STEP = 3600000L; // 1 hour
    private static final String MISSING_COLUMN_VALUE = "-";

    public static void main(String[] args) {
        RefactorRetentionOptions opts = new RefactorRetentionOptions(args);
        try {
            opts.checkArgs();
            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to refactor identified consumer groups based on consumption.");

            // should have exactly one action
            int actions = 0;
            if (opts.options.has(opts.groupOpt)) actions++;
            if (opts.options.has(opts.allGroupsOpt)) actions++;
            
            if (actions != 1) {
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one consumer scope: --group --all-groups");
            }

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    public static void run(RefactorRetentionOptions opts) {
        boolean verbose = opts.options.has(opts.verboseOpt);
        boolean execute = opts.options.has(opts.executeOpt);
        String confluentGroupPrefix = opts.options.has(opts.confluentGroupPrefixOpt) 
            ? opts.options.valueOf(opts.confluentGroupPrefixOpt) 
            : DEFAULT_CONFLUENT_GROUP_PREFIX;

        ConsumerGroupService consumerGroupService = new ConsumerGroupService(opts);
        try {
            List<String> groups = consumerGroupService.getGroups(confluentGroupPrefix);
            TreeMap<String, GroupOffsetInfo> offsets = consumerGroupService.collectGroupsOffsets(groups);
            
            if (verbose) {
                System.out.println("We have offsets, now compute stuff");
            }

            // Process offsets to find oldest consumed messages per topic partition
            Map<TopicPartitionKey, List<TopicPartitionOffset>> topicPartitionOffsets = new HashMap<>();
            
            for (Map.Entry<String, GroupOffsetInfo> entry : offsets.entrySet()) {
                String groupId = entry.getKey();
                GroupOffsetInfo groupInfo = entry.getValue();
                
                if (groupInfo.assignments != null) {
                    for (PartitionAssignmentState partition : groupInfo.assignments) {
                        if (partition.topic != null && partition.partition != null && partition.offset != null) {
                            String topic = partition.topic;
                            long topicPartitions = consumerGroupService.getTopicPartitionDetails(topic);
                            int p = partition.partition;
                            long offset = partition.offset;
                            
                            // Since Kafka Protocol commits to one past the last message received,
                            // we must go back one message to identify the timestamp of the message pulled
                            if (offset > 0) offset -= 1;

                            if (verbose) {
                                System.out.printf("GroupID: %s -- Topic %s -- Partitions %d -- Partition %d -- Offset %d%n", 
                                    groupId, topic, topicPartitions, p, offset);
                            }
                            
                            TopicPartitionKey key = new TopicPartitionKey(topic, topicPartitions, p);
                            TopicPartitionOffset tpo = new TopicPartitionOffset(topic, topicPartitions, p, offset);
                            
                            topicPartitionOffsets.computeIfAbsent(key, k -> new ArrayList<>()).add(tpo);
                        }
                    }
                }
            }

            // Build map of oldest offsets per topic/partition
            Map<String, Long> oldestTopicPartitionOffsets = new HashMap<>();
            String tpJoiner = "_Partition:";
            
            for (Map.Entry<TopicPartitionKey, List<TopicPartitionOffset>> entry : topicPartitionOffsets.entrySet()) {
                for (TopicPartitionOffset tpo : entry.getValue()) {
                    String tp = tpo.topic + tpJoiner + tpo.partition;
                    oldestTopicPartitionOffsets.merge(tp, tpo.offset, Math::min);
                }
            }

            // Find oldest timestamps for each topic/partition pair
            if (verbose) {
                System.out.println("Find the oldest Timestamps for each Topic/Partition Pair");
            }

            KafkaConsumer<String, String> consumer = createKafkaClient(opts, new HashMap<>());
            Map<String, Long> oldestTopicTimestamp = new HashMap<>();
            Set<String> foundTP = new HashSet<>();

            for (Map.Entry<String, Long> entry : oldestTopicPartitionOffsets.entrySet()) {
                String tp = entry.getKey();
                long offset = entry.getValue();
                String[] parts = tp.split(tpJoiner);
                String topic = parts[0];
                int partition = Integer.parseInt(parts[1]);

                foundTP.add(tp);

                long timestamp = getKafkaRecordTimestamp(consumer, topic, partition, offset);
                if (verbose) {
                    System.out.printf("Topic: %s Partition: %d Offset: %d Timestamp: %d%n", topic, partition, offset, timestamp);
                }

                if (timestamp == -1) {
                    System.out.printf("Skipping invalid timestamp for Topic: %s Partition: %d Offset: %d%n", topic, partition, offset);
                } else {
                    oldestTopicTimestamp.merge(topic, timestamp, Math::min);
                }
            }

            // Find partitions that do not have any consumer offsets
            if (verbose) {
                System.out.println("Find partitions that do not have any consumer offsets.");
            }

            Set<String> availableTP = new HashSet<>();
            Set<String> topics = foundTP.stream()
                .map(tp -> tp.split(tpJoiner)[0])
                .collect(Collectors.toSet());

            for (String topic : topics) {
                long partitions = consumerGroupService.getTopicPartitionDetails(topic) - 1;
                for (int x = 0; x <= partitions; x++) {
                    availableTP.add(topic + tpJoiner + x);
                }
            }

            Set<String> deltaTP = new HashSet<>(availableTP);
            deltaTP.removeAll(foundTP);

            if (verbose) {
                System.out.println("deltaTP = availableTP diff foundTP");
                System.out.println(deltaTP.toString());
            }

            Map<String, Set<Integer>> missingTopicPartitions = new HashMap<>();
            for (String tp : deltaTP) {
                String[] parts = tp.split(tpJoiner);
                String topic = parts[0];
                int partition = Integer.parseInt(parts[1]);
                missingTopicPartitions.computeIfAbsent(topic, k -> new HashSet<>()).add(partition);
            }

            Map<String, Map<String, Long>> topicRetention = computeRetentionTime(opts, oldestTopicTimestamp, missingTopicPartitions, consumerGroupService);
            printRetention(topicRetention);

            if (execute) {
                consumerGroupService.setTopicRetentionTime(topicRetention);
            }

        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Exception e) {
            printError("Executing consumer group command failed due to " + e.getMessage(), e);
        } finally {
            consumerGroupService.close();
        }
    }

    private static void printRetention(Map<String, Map<String, Long>> topicRetention) {
        if (topicRetention.isEmpty()) {
            System.out.println("No topics found to compute retention for. Perhaps the data has already been removed.");
        } else {
            System.out.printf("%n%-60s %-16s %-16s%n", "TOPIC", "RETENTION", "NEW-RETENTION");
        }

        for (Map.Entry<String, Map<String, Long>> entry : topicRetention.entrySet()) {
            String topic = entry.getKey();
            Map<String, Long> retention = entry.getValue();
            System.out.printf("%-60s %-16s %-16s%n",
                topic,
                retention.getOrDefault("current", -1L),
                retention.getOrDefault("proposed", -1L));
        }
    }

    private static KafkaConsumer<String, String> createKafkaClient(RefactorRetentionOptions opts, Map<String, String> configOverrides) {
        Properties props;
        try {
            props = opts.options.has(opts.commandConfigOpt) 
                ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) 
                : new Properties();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load command config properties", e);
        }
        
        props.put("bootstrap.servers", opts.options.valueOf(opts.bootstrapServerOpt));
        configOverrides.forEach(props::put);
        props.put("group.id", "RefactorRetentionClient");
        props.put("key.deserializer", BytesDeserializer.class.getName());
        props.put("value.deserializer", BytesDeserializer.class.getName());
        
        return new KafkaConsumer<>(props);
    }

    private static long getKafkaRecordTimestamp(KafkaConsumer<String, String> consumer, String topic, int partition, long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        List<TopicPartition> tpCollection = Collections.singletonList(tp);
        consumer.assign(tpCollection);
        consumer.seek(tp, offset);

        long timestamp = 0L;
        try {
            var records = consumer.poll(Duration.ofSeconds(2));
            if (!records.isEmpty()) {
                var record = records.iterator().next();
                timestamp = record.timestamp();
            } else {
                timestamp = -1L;
            }
        } catch (InvalidOffsetException | IllegalArgumentException e) {
            timestamp = -1L;
        }

        return timestamp;
    }



    private static void printError(String msg, Exception e) {
        System.out.println("\nError: " + msg);
        if (e != null) {
            e.printStackTrace();
        }
    }

    private static Map<String, Map<String, Long>> computeRetentionTime(
            RefactorRetentionOptions opts,
            Map<String, Long> oldestTopicTimestamp,
            Map<String, Set<Integer>> missingTopicPartitions,
            ConsumerGroupService consumerGroupService) {

        Map<String, Map<String, Long>> topicRetention = new HashMap<>();
        boolean verbose = opts.options.has(opts.verboseOpt);
        boolean ignoreMissingPartitions = opts.options.has(opts.ignoreMissingPartitionsOpt);

        if (opts.options.has(opts.retentionMsOpt)) {
            System.out.println("TODO: Implement Exact Retention Strategy");
        } else {
            long minRetention = opts.options.has(opts.retentionMinMsOpt) 
                ? opts.options.valueOf(opts.retentionMinMsOpt) 
                : DEFAULT_RETENTION_MIN;
            long maxRetention = opts.options.has(opts.retentionMaxMsOpt) 
                ? opts.options.valueOf(opts.retentionMaxMsOpt) 
                : DEFAULT_RETENTION_MAX;
            long stepRetention = opts.options.has(opts.retentionStepMsOpt) 
                ? opts.options.valueOf(opts.retentionStepMsOpt) 
                : DEFAULT_RETENTION_STEP;

            if (verbose) {
                System.out.println("----------------------------------------");
                System.out.println("Retention Values to use for Computations");
                System.out.printf("Min : %d%n", minRetention);
                System.out.printf("Max : %d%n", maxRetention);
                System.out.printf("Step: %d%n", stepRetention);
            }

            long now = Instant.now().toEpochMilli();

            for (Map.Entry<String, Long> entry : oldestTopicTimestamp.entrySet()) {
                String topic = entry.getKey();
                long oldestTimestamp = entry.getValue();
                long currentRetention = consumerGroupService.getTopicRetentionTime(topic);
                long delta = now - oldestTimestamp;
                long proposedRetention = minRetention;
                boolean missingPartitions = missingTopicPartitions.containsKey(topic);

                if (verbose) {
                    System.out.printf("Topic: %s CurrentRetention: %d NowTS: %d OldestTS: %d Delta: %d%n", 
                        topic, currentRetention, now, oldestTimestamp, delta);
                }

                if (missingPartitions && ignoreMissingPartitions) {
                    System.out.printf("WARN: Topic [%s] has partitions that consumers are not consuming.%n", topic);
                    System.out.println("Ignoring the missing partitions due to --ignore-missing-partitions being set");
                }

                if (missingPartitions && !ignoreMissingPartitions) {
                    System.out.printf("WARN: Topic [%s] has partitions that consumers are not consuming.%n", topic);
                    System.out.println("The topic's retention will not be changed unless --ignore-missing-partitions is set");
                    proposedRetention = currentRetention;
                } else if (currentRetention < minRetention) {
                    if (verbose) System.out.println("Choosing minRetention since current is lower");
                    proposedRetention = minRetention;
                } else {
                    long r = minRetention;
                    boolean done = false;
                    while (r < maxRetention && !done) {
                        if (verbose) System.out.printf("Checking %d >= %d%n", r, delta);
                        if (r >= delta) {
                            if (verbose) System.out.printf("Using retention %d%n", r);
                            proposedRetention = r;
                            done = true;
                        }
                        r += stepRetention;
                    }
                    if (!done || proposedRetention > maxRetention) {
                        if (verbose) System.out.println("Oldest timestamp was too great, set to max");
                        proposedRetention = maxRetention;
                    }
                }

                Map<String, Long> retentionInfo = new HashMap<>();
                retentionInfo.put("current", currentRetention);
                retentionInfo.put("proposed", proposedRetention);
                retentionInfo.put("delta", delta);
                retentionInfo.put("minRetention", minRetention);
                retentionInfo.put("maxRetention", maxRetention);
                retentionInfo.put("stepRetention", stepRetention);
                
                topicRetention.put(topic, retentionInfo);
            }
        }

        return topicRetention;
    }

    // Helper classes
    static class TopicPartitionOffset {
        final String topic;
        final long partitions;
        final int partition;
        final long offset;

        TopicPartitionOffset(String topic, long partitions, int partition, long offset) {
            this.topic = topic;
            this.partitions = partitions;
            this.partition = partition;
            this.offset = offset;
        }
    }

    static class TopicPartitionKey {
        final String topic;
        final long partitions;
        final int partition;

        TopicPartitionKey(String topic, long partitions, int partition) {
            this.topic = topic;
            this.partitions = partitions;
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicPartitionKey that = (TopicPartitionKey) o;
            return partitions == that.partitions && partition == that.partition && topic.equals(that.topic);
        }

        @Override
        public int hashCode() {
            return topic.hashCode() * 31 * 31 + (int) (partitions * 31) + partition;
        }
    }

    static class PartitionAssignmentState {
        final String group;
        final Optional<Node> coordinator;
        final String topic;
        final Integer partition;
        final Long offset;
        final Long lag;
        final String consumerId;
        final String host;
        final String clientId;
        final Long logEndOffset;

        PartitionAssignmentState(String group, Optional<Node> coordinator, String topic, Integer partition, 
                                Long offset, Long lag, String consumerId, String host, String clientId, Long logEndOffset) {
            this.group = group;
            this.coordinator = coordinator;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.lag = lag;
            this.consumerId = consumerId;
            this.host = host;
            this.clientId = clientId;
            this.logEndOffset = logEndOffset;
        }
    }

    static class GroupOffsetInfo {
        final String state;
        final List<PartitionAssignmentState> assignments;

        GroupOffsetInfo(String state, List<PartitionAssignmentState> assignments) {
            this.state = state;
            this.assignments = assignments;
        }
    }

    static class ConsumerGroupService {
        private final RefactorRetentionOptions opts;
        private final Admin adminClient;
        private final boolean verbose;
        private final Map<String, TopicDescription> topicDetails = new HashMap<>();

        ConsumerGroupService(RefactorRetentionOptions opts) {
            this(opts, new HashMap<>());
        }

        ConsumerGroupService(RefactorRetentionOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            this.adminClient = createAdminClient(configOverrides);
            this.verbose = opts.options.has(opts.verboseOpt);
        }

        long getTopicPartitionDetails(String topic) {
            TopicDescription topicDescription = topicDetails.get(topic);
            if (topicDescription == null) {
                try {
                    var describeTopicsResult = adminClient.describeTopics(
                        Collections.singletonList(topic), 
                        withTimeoutMs(new DescribeTopicsOptions()));
                    topicDescription = describeTopicsResult.topicNameValues().get(topic).get();
                    topicDetails.put(topic, topicDescription);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to describe topic: " + topic, e);
                }
            }
            return topicDescription.partitions().size();
        }

        @SuppressWarnings({"deprecation", "removal"})
        List<String> listConsumerGroups() {
            try {
                var result = adminClient.listConsumerGroups();
                return result.all().get().stream()
                    .map(listing -> listing.groupId())
                    .collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to list consumer groups", e);
            }
        }

        List<String> getGroups(String confluentGroupPrefix) {
            List<String> allGroups = listConsumerGroups();
            Pattern pattern = Pattern.compile(confluentGroupPrefix);
            
            if (verbose) {
                System.out.println("\nAll Groups before filtering Confluent Groups:");
                System.out.println("----------------------------------------");
                if (allGroups.isEmpty()) {
                    System.out.println("No Groups");
                } else {
                    allGroups.forEach(System.out::println);
                }
                System.out.println("----------------------------------------");
            }

            List<String> groupsBeforeFilter = allGroups.stream()
                .filter(group -> !pattern.matcher(group).matches())
                .collect(Collectors.toList());

            String groupFilter = opts.options.valueOf(opts.groupOpt);
            if (verbose) {
                System.out.printf("\nGroupFilter: %s%n", groupFilter);
                System.out.println("Groups before supplied filter:");
                System.out.println("----------------------------------------");
                if (groupsBeforeFilter.isEmpty()) {
                    System.out.println("No Groups");
                } else {
                    groupsBeforeFilter.forEach(System.out::println);
                }
                System.out.println("----------------------------------------");
            }

            List<String> groups;
            if (groupFilter != null && !groupFilter.isEmpty()) {
                if (verbose) System.out.println("Applying filter...");
                Pattern groupPattern = Pattern.compile(groupFilter);
                groups = groupsBeforeFilter.stream()
                    .filter(group -> groupPattern.matcher(group).matches())
                    .collect(Collectors.toList());
            } else {
                groups = groupsBeforeFilter;
            }

            if (verbose && !groups.isEmpty()) {
                System.out.println("\nGroups after supplied filter:");
                System.out.println("----------------------------------------");
                groups.forEach(System.out::println);
                System.out.println("----------------------------------------");
            } else if (groups.isEmpty()) {
                System.out.printf("No Groups found for [%s]%n", groupFilter);
            }

            return groups;
        }

        TreeMap<String, GroupOffsetInfo> collectGroupsOffsets(List<String> groupIds) {
            try {
                Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
                TreeMap<String, GroupOffsetInfo> groupOffsets = new TreeMap<>();

                for (Map.Entry<String, ConsumerGroupDescription> entry : consumerGroups.entrySet()) {
                    String groupId = entry.getKey();
                    ConsumerGroupDescription consumerGroup = entry.getValue();
                    @SuppressWarnings("deprecation")
                    String state = consumerGroup.state().name();
                    Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommittedOffsets(groupId);

                    List<PartitionAssignmentState> assignments = new ArrayList<>();
                    for (MemberDescription member : consumerGroup.members()) {
                        if (!member.assignment().topicPartitions().isEmpty()) {
                            for (TopicPartition tp : member.assignment().topicPartitions()) {
                                OffsetAndMetadata offsetAndMetadata = committedOffsets.get(tp);
                                Long offset = offsetAndMetadata != null ? offsetAndMetadata.offset() : null;
                                
                                PartitionAssignmentState assignment = new PartitionAssignmentState(
                                    groupId,
                                    Optional.of(consumerGroup.coordinator()),
                                    tp.topic(),
                                    tp.partition(),
                                    offset,
                                    null, // lag calculation omitted for simplicity
                                    member.consumerId(),
                                    member.host(),
                                    member.clientId(),
                                    null // logEndOffset omitted for simplicity
                                );
                                assignments.add(assignment);
                            }
                        }
                    }

                    groupOffsets.put(groupId, new GroupOffsetInfo(state, assignments));
                }

                return groupOffsets;
            } catch (Exception e) {
                throw new RuntimeException("Failed to collect group offsets", e);
            }
        }

        private Map<String, ConsumerGroupDescription> describeConsumerGroups(List<String> groupIds) {
            try {
                return adminClient.describeConsumerGroups(groupIds, withTimeoutMs(new DescribeConsumerGroupsOptions()))
                    .describedGroups()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            try {
                                return entry.getValue().get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    ));
            } catch (Exception e) {
                throw new RuntimeException("Failed to describe consumer groups", e);
            }
        }

        private Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(String groupId) {
            try {
                return adminClient.listConsumerGroupOffsets(
                    Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec()),
                    withTimeoutMs(new ListConsumerGroupOffsetsOptions())
                ).partitionsToOffsetAndMetadata(groupId).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to get committed offsets for group: " + groupId, e);
            }
        }

        long getTopicRetentionTime(String topic) {
            try {
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                var configs = adminClient.describeConfigs(Collections.singleton(configResource))
                    .all().get(30, TimeUnit.SECONDS);
                
                return configs.get(configResource).entries().stream()
                    .filter(entry -> "retention.ms".equals(entry.name()))
                    .mapToLong(entry -> Long.parseLong(entry.value()))
                    .findFirst()
                    .orElse(0L);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException("Failed to get retention time for topic: " + topic, e);
            }
        }

        void setTopicRetentionTime(Map<String, Map<String, Long>> topicRetention) {
            try {
                Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
                
                for (Map.Entry<String, Map<String, Long>> entry : topicRetention.entrySet()) {
                    String topic = entry.getKey();
                    Map<String, Long> details = entry.getValue();
                    Long proposed = details.get("proposed");
                    
                    if (proposed != null && proposed > 0L) {
                        System.out.printf("Setting retention for %s to %d.%n", topic, proposed);
                        List<AlterConfigOp> ops = Collections.singletonList(
                            new AlterConfigOp(new ConfigEntry("retention.ms", proposed.toString()), AlterConfigOp.OpType.SET)
                        );
                        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), ops);
                    } else {
                        System.out.printf("Skipping setting retention for %s due to a bad proposed value.%n", topic);
                    }
                }

                adminClient.incrementalAlterConfigs(configs).all().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to set topic retention times", e);
            }
        }

        void close() {
            adminClient.close();
        }

        private Admin createAdminClient(Map<String, String> configOverrides) {
            Properties props;
            try {
                props = opts.options.has(opts.commandConfigOpt) 
                    ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) 
                    : new Properties();
            } catch (IOException e) {
                throw new RuntimeException("Failed to load command config properties", e);
            }
            
            props.put("bootstrap.servers", opts.options.valueOf(opts.bootstrapServerOpt));
            configOverrides.forEach(props::put);
            return Admin.create(props);
        }

        private <T extends org.apache.kafka.clients.admin.AbstractOptions<T>> T withTimeoutMs(T options) {
            long timeout = opts.options.valueOf(opts.timeoutMsOpt);
            return options.timeoutMs((int) timeout);
        }
    }

    static class RefactorRetentionOptions extends CommandDefaultOptions {
        final OptionSpec<String> bootstrapServerOpt;
        final OptionSpec<Long> timeoutMsOpt;
        final OptionSpec<String> groupOpt;
        final OptionSpec<String> allGroupsOpt;
        final OptionSpec<String> confluentGroupPrefixOpt;
        final OptionSpec<String> topicOpt;
        final OptionSpec<Long> retentionMsOpt;
        final OptionSpec<Long> retentionMinMsOpt;
        final OptionSpec<Long> retentionMaxMsOpt;
        final OptionSpec<Long> retentionStepMsOpt;
        final OptionSpec<String> commandConfigOpt;
        final OptionSpec<Void> executeOpt;
        final OptionSpec<Void> ignoreMissingPartitionsOpt;
        final OptionSpec<Void> verboseOpt;

        RefactorRetentionOptions(String[] args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);

            timeoutMsOpt = parser.accepts("timeout", 
                "The timeout that can be set for some use cases. For example, it can be used when describing the group to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, or is going through some changes).")
                .withRequiredArg()
                .describedAs("timeout (ms)")
                .ofType(Long.class)
                .defaultsTo(5000L);

            groupOpt = parser.accepts("group", "The consumer group(s) we wish to act on. Example: --group enrich-*,route-*,ext-*")
                .withRequiredArg()
                .describedAs("consumer group")
                .ofType(String.class);

            allGroupsOpt = parser.accepts("all-groups", "All consumer group(s) besides internal, ksql, and connect will be included.")
                .withRequiredArg()
                .describedAs("consumer group")
                .ofType(String.class);

            confluentGroupPrefixOpt = parser.accepts("confluent-prefix", "The consumer group prefix that Confluent components will utilize")
                .withRequiredArg()
                .describedAs("confluent reserved consumer group")
                .ofType(String.class)
                .defaultsTo(DEFAULT_CONFLUENT_GROUP_PREFIX);

            topicOpt = parser.accepts("topic", "The specific topic(s) we want to apply the changes to.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);

            retentionMsOpt = parser.accepts("retention", "Specific retention (milliseconds) time for a topic (default = 4 days)")
                .withRequiredArg()
                .describedAs("timeout (ms)")
                .ofType(Long.class)
                .defaultsTo(DEFAULT_RETENTION);

            retentionMinMsOpt = parser.accepts("retention-min", "Minimum retention (milliseconds) time for a topic (default = 1 hour)")
                .withRequiredArg()
                .describedAs("timeout (ms)")
                .ofType(Long.class)
                .defaultsTo(DEFAULT_RETENTION_MIN);

            retentionMaxMsOpt = parser.accepts("retention-max", "Maximum retention (milliseconds) time for a topic (default = 7 days)")
                .withRequiredArg()
                .describedAs("timeout (ms)")
                .ofType(Long.class)
                .defaultsTo(DEFAULT_RETENTION_MAX);

            retentionStepMsOpt = parser.accepts("retention-step", "Amount of (milliseconds) time to reduce/increase a topic by (default = 1 hour)")
                .withRequiredArg()
                .describedAs("timeout (ms)")
                .ofType(Long.class)
                .defaultsTo(DEFAULT_RETENTION_STEP);

            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client and Consumer.")
                .withRequiredArg()
                .describedAs("command config property file")
                .ofType(String.class);

            executeOpt = parser.accepts("execute", "Execute operation. If the user does not --execute the plan will be exported, but nothing will actually change.");

            ignoreMissingPartitionsOpt = parser.accepts("ignore-missing-partitions", 
                "Some consumers will not be consuming all partitions of a topic. In these circumstances this script will attempt to keep what is already set. Use this setting to force refactoring of the topic.");

            verboseOpt = parser.accepts("verbose", "Provide additional information, if any, when describing the group or calculating the retention times.");

            options = parser.parse(args);
        }

        void checkArgs() {
            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);

            if (options.has(groupOpt) && options.has(allGroupsOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "Option --group may not be combined with --all-groups");
            }

            if (options.has(retentionMsOpt) && 
                (options.has(retentionMinMsOpt) || options.has(retentionMaxMsOpt) || options.has(retentionStepMsOpt))) {
                CommandLineUtils.printUsageAndExit(parser, "Option --retention may not be combined with --retention-min, --retention-max, or --retention-step");
            }

            if (!options.has(executeOpt)) {
                System.err.println("WARN: No action will be performed as the --execute option is missing.");
            }
        }
    }
}