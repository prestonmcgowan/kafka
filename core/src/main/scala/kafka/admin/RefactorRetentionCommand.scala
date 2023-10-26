package kafka.admin

import java.lang.IllegalArgumentException
import java.time.{Duration, Instant}
import java.util.{ArrayList, Collection, Collections, Properties}
import java.util.concurrent.TimeUnit
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{InvalidOffsetException, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, immutable, mutable}
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.util.{Failure, Success, Try}
import joptsimple.{OptionException, OptionSpec}
import org.apache.kafka.common.protocol.Errors

import scala.collection.immutable.TreeMap
import org.apache.kafka.common.ConsumerGroupState

// Unused imports for now
// import java.time.{Duration, Instant}
// 
// import com.fasterxml.jackson.dataformat.csv.CsvMapper
// import com.fasterxml.jackson.module.scala.DefaultScalaModule
// import org.apache.kafka.common.{KafkaException, Node, TopicPartition}
// 
// import scala.reflect.ClassTag
// import org.apache.kafka.common.requests.ListOffsetsResponse

object RefactorRetentionCommand extends Logging {

  val CONFLUENT_GROUP_PREFIX = "^_confluent-.*"
  val DEFAULT_RETENTION = 345600000L
  val DEFAULT_RETENTION_MIN = 3600000L
  val DEFAULT_RETENTION_MAX = 604800000L
  val DEFAULT_RETENTION_STEP = 3600000L
  

  case class TopicPartitionOffset(topic: String, partition: Int, offset: Long) extends Product with Serializable
  def main(args: Array[String]): Unit = {

    val opts = new RefactorRetentionCommandOptions(args)
    try {
      opts.checkArgs()
      CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to refactor identified consumer groups based on consumption.")

      // should have exactly one action
      val actions = Seq(opts.groupOpt, opts.allGroupsOpt).count(opts.options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one consumer scope: --group --all-groups")

      run(opts)
    } catch {
      case e: OptionException =>
        CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage)
    }
  }

  def run(opts: RefactorRetentionCommandOptions): Unit = {
    // Retention Computation Process
    // 1. (getGroups)                Identify the groups we need to pull data for
    // 2. (getTopics)                Pull the topics for each consumer
    // 3. (getTopicOffsets)          For each $consumer[$topic[$partition]], pull the offset
    // 4. (mapReduceTopicOffsets)    Now that all $consumer[$topic[$partition[$offset]]], identify the oldest offset for each partition
    // 5. (getPartitionTimestamp)    Pull the timestamp for each of the oldest offsets
    // 6. (mapReduceTopicTimestamps) Flatten the oldest offset to identify the topic's oldest timestamp
    // 7. (applyRetentionPolicy)     Math out the retention time recommendation with the topic's current lag and the provided retention settings
    // 8. (printRetentionPlan)       Print out the retention plan based on the above math
    // 9. (executeRetentionPlan)     Execute the plan if the argument was provided

    val verbose = opts.options.has(opts.verboseOpt)
    val execute = opts.options.has(opts.executeOpt)
    val consumerGroupService = new ConsumerGroupService(opts)
    try {
      val offsets = consumerGroupService.collectGroupsOffsets(consumerGroupService.getGroups(CONFLUENT_GROUP_PREFIX))
      if (verbose) println(f"We have offsets, not compute stuff")
      
      // topicPartitionOffsets contains [topic: "name", offsets: [ partition, offset ]]
      val topicPartitionOffsets = 
        offsets.map{ case(groupId, (state, assignments)) =>
          assignments.map{ partitions =>
            partitions.map{ partitionDetails =>
              val topic = partitionDetails.topic.get
              val p = partitionDetails.partition.get
              val f = partitionDetails.offset.get
            
              if (verbose) println(f"GroupID: $groupId -- Topic $topic -- Partition $p -- Offset $f")
              TopicPartitionOffset(topic, p, f)
            }
          }
        }.flatten.flatten.groupBy(tpo => (tpo.topic, tpo.partition))

      // TODO: Likely a better way to do this
      val tpJoiner = "_Partition:"
      val oldestTopicPartitionOffsets = new HashMap[String, Long]()
      topicPartitionOffsets.foreach{ case ( (topic, partition), tpoList) =>
        tpoList.foreach{ tpo =>
          val tp = tpo.topic + tpJoiner + tpo.partition
          if (oldestTopicPartitionOffsets.contains(tp)) {
            if (oldestTopicPartitionOffsets(tp) > tpo.offset) {
              oldestTopicPartitionOffsets += (tp -> tpo.offset)
            }
          } else {
            oldestTopicPartitionOffsets += (tp -> tpo.offset)
          }
        }
      }

      // Now that we have the topics, partitions, and old offsets...
      // Pull the timestamp for the message to determine the oldest timestamps
      val configOverrides = new HashMap[String, String]()
      val consumer = createKafkaClient(opts, configOverrides)
      val oldestTopicTimestamp = new HashMap[String, Long]()
      oldestTopicPartitionOffsets.foreach{ case (tp, offset) =>
        val parts = tp.split(tpJoiner)
        val topic = parts(0)
        val partition = parts(1).toInt
        val timestamp = getKafkaRecordTimestamp(consumer, topic, partition, offset)
        if (verbose) println(f"Topic: $topic Partition: $partition Offset: $offset Timestamp: $timestamp")
        if (timestamp == -1) {
          println(f"Skipping invalid timestamp for Topic: $topic Partition: $partition Offset: $offset")
        } else if (oldestTopicTimestamp.contains(topic)) {
          if (oldestTopicTimestamp(topic) > timestamp) {
            oldestTopicTimestamp += (topic -> timestamp)
          }
        } else {
          oldestTopicTimestamp += (topic -> timestamp)
        }
      }
      
        val topicRetention = computeRetentionTime(opts, oldestTopicTimestamp, consumerGroupService)

        printRetention(topicRetention)

        if (execute) {
          consumerGroupService.setTopicRetentionTime(topicRetention)
        }


      // } else {
      //   println(f"No retention settings provided. Please specify --retention, --retention-min OR --retention-max, --retention-step")
      // }
      


      
    } catch {
      case e: IllegalArgumentException =>
        CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage)
      case e: Throwable =>
        printError(s"Executing consumer group command failed due to ${e.getMessage}", Some(e))
    } finally {
      consumerGroupService.close()
    }
  }


  def printRetention(topicRetention: HashMap[String, HashMap[String, Long]]): Unit = {
    if (topicRetention.nonEmpty)
      println("\n%-60s %-16s %-16s".format("TOPIC", "RETENTION", "NEW-RETENTION"))
    for {
      (topic, topicRetention) <- topicRetention
    } {
      println("%-60s %-16s %-16s".format(
        topic,
        topicRetention.get("current").getOrElse("ERROR"),
        topicRetention.get("proposed").getOrElse("ERROR")
      ))
    }
  }

  def createKafkaClient(opts: RefactorRetentionCommandOptions, configOverrides: Map[String, String]): KafkaConsumer[String, String] = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      configOverrides.forKeyValue { (k, v) => props.put(k, v)}
      props.put("group.id", "RefactorRetentionClient")
      props.put("key.deserializer", classOf[BytesDeserializer])
      props.put("value.deserializer", classOf[BytesDeserializer])
      val kafkaConsumer = new KafkaConsumer[String, String](props)
      kafkaConsumer
    }

  def getKafkaRecordTimestamp(consumer: KafkaConsumer[String, String], topic: String, partition: Int, offset: Long) : Long = {
    
    val tp = new TopicPartition(topic, partition)
    val tpCollection = new ArrayList[TopicPartition]()
    tpCollection.add(tp)
    consumer.assign(tpCollection)
    consumer.seek(tp, offset)

    var timestamp = 0L
    try {
      val records = consumer.poll(Duration.ofSeconds(2))
      if (! records.isEmpty()) {
        val record = records.iterator().next()
        timestamp = record.timestamp()
      } else {
        timestamp = -1L
      }
    } catch {
      case e: InvalidOffsetException =>
        timestamp = -1L
      case e: IllegalArgumentException =>
        timestamp = -1L
    }

    // 
    // timestamp = record.timestamp()
    timestamp
  }

  def consumerGroupStatesFromString(input: String): Set[ConsumerGroupState] = {
    val parsedStates = input.split(',').map(s => ConsumerGroupState.parse(s.trim)).toSet
    if (parsedStates.contains(ConsumerGroupState.UNKNOWN)) {
      val validStates = ConsumerGroupState.values().filter(_ != ConsumerGroupState.UNKNOWN)
      throw new IllegalArgumentException(s"Invalid state list '$input'. Valid states are: ${validStates.mkString(", ")}")
    }
    parsedStates
  }

  val MISSING_COLUMN_VALUE = "-"

  def printError(msg: String, e: Option[Throwable] = None): Unit = {
    println(s"\nError: $msg")
    e.foreach(_.printStackTrace())
  }

  def printOffsetsToReset(groupAssignmentsToReset: Map[String, Map[TopicPartition, OffsetAndMetadata]]): Unit = {
    if (groupAssignmentsToReset.nonEmpty)
      println("\n%-30s %-30s %-10s %-15s".format("GROUP", "TOPIC", "PARTITION", "NEW-OFFSET"))
    for {
      (groupId, assignment) <- groupAssignmentsToReset
      (consumerAssignment, offsetAndMetadata) <- assignment
    } {
      println("%-30s %-30s %-10s %-15s".format(
        groupId,
        consumerAssignment.topic,
        consumerAssignment.partition,
        offsetAndMetadata.offset))
    }
  }

  private[admin] case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                                partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                                consumerId: Option[String], host: Option[String],
                                                clientId: Option[String], logEndOffset: Option[Long])

  private[admin] case class MemberAssignmentState(group: String, consumerId: String, host: String, clientId: String, groupInstanceId: String,
                                             numPartitions: Int, assignment: List[TopicPartition])

  private[admin] case class GroupState(group: String, coordinator: Node, assignmentStrategy: String, state: String, numMembers: Int)

  def computeRetentionTime(opts: RefactorRetentionCommandOptions, oldestTopicTimestamp:HashMap[String, Long], consumerGroupService: ConsumerGroupService ): HashMap[String, HashMap[String, Long]] = {

    // Determine the retention math strategy we will use
    // exact retention vs computed retention
    val topicRetention = new HashMap[String, HashMap[String, Long]]()

    if (opts.options.has(opts.retentionMsOpt)) {
      // val exactRetention = opts.retentionMsOpt
      println("TODO: Implement Exact Retention Strategy")

    } else if  (
        opts.options.has(opts.retentionMinMsOpt) ||
        opts.options.has(opts.retentionMaxMsOpt) 
    ) {
      // TODO: Better Scala way to do this? getOrElse perhaps
      val minRetention  = if (opts.options.has(opts.retentionMinMsOpt)) opts.options.valueOf(opts.retentionMinMsOpt) else DEFAULT_RETENTION_MIN
      val maxRetention  = if (opts.options.has(opts.retentionMaxMsOpt)) opts.options.valueOf(opts.retentionMaxMsOpt) else DEFAULT_RETENTION_MAX
      val stepRetention = if (opts.options.has(opts.retentionStepMsOpt)) opts.options.valueOf(opts.retentionStepMsOpt) else DEFAULT_RETENTION_STEP

      // We will utilize the user's retention values and the topic's oldest timestamp
      val now = Instant.now().toEpochMilli()
        
      oldestTopicTimestamp.foreach{ case(topic, oldestTimestamp) =>
        val currentRetention = consumerGroupService.getTopicRetentionTime(topic)
        val delta = now - oldestTimestamp
        var proposedRetention = currentRetention
        if (currentRetention < minRetention) {
          proposedRetention = minRetention
        } else {
          var r = minRetention
          var done = false
          while (r < maxRetention && !done) {
            if (r >= delta) {
              proposedRetention = r
              done = true
            }
            r += stepRetention
          }
          if (!done) {
            // Oldest timestamp was too great, set to max
            proposedRetention = maxRetention
          }
        
        }

        topicRetention.addOne(topic, HashMap(
          "current" -> currentRetention,
          "proposed" -> proposedRetention,
          "delta" -> delta,
          "minRetention" -> minRetention,
          "maxRetention" -> maxRetention,
          "stepRetention" -> stepRetention
        ))
      }
    }
    topicRetention
  }


  class ConsumerGroupService(val opts: RefactorRetentionCommandOptions,
                             private[admin] val configOverrides: Map[String, String] = Map.empty) {

    private val adminClient = createAdminClient(configOverrides)
    private val verbose = opts.options.has(opts.verboseOpt)

    def getRetentionTime(topic: String): Unit = {

    }

    def listConsumerGroups(): List[String] = {
      val result = adminClient.listConsumerGroups(withTimeoutMs(new ListConsumerGroupsOptions))
      val listings = result.all.get.asScala
      listings.map(_.groupId).toList
    }

    def listConsumerGroupsWithState(states: Set[ConsumerGroupState]): List[ConsumerGroupListing] = {
      val listConsumerGroupsOptions = withTimeoutMs(new ListConsumerGroupsOptions())
      listConsumerGroupsOptions.inStates(states.asJava)
      val result = adminClient.listConsumerGroups(listConsumerGroupsOptions)
      result.all.get.asScala.toList
    }
    
    def getGroups(confluentGroupPrefix: String): List[String] = {
      val allGroups = this.listConsumerGroups()
      if (verbose) {
        println("")
        println("All Groups before filtering Confluent Groups:")
        println("----------------------------------------")
        if (allGroups.length <= 0)
          println("No Groups")
        else allGroups.foreach(println(_))
        println("----------------------------------------")
      }
      val groupsBeforeFilter = allGroups.filter(!_.matches(confluentGroupPrefix))
      val groupFilter = opts.options.valueOf(opts.groupOpt)
      if (verbose) {
        println("")
        println("GroupFilter: " + groupFilter)
        println("Groups before supplied filter:")
        println("----------------------------------------")
        if (allGroups.length <= 0)
          println("No Groups")
        else allGroups.foreach(println(_))
        println("----------------------------------------")
      }

      val groups =
        if (groupFilter.length > 0) {
          if (verbose) println("Applying filter...")
          groupsBeforeFilter.filter(_.matches(groupFilter))
        }
        else 
          groupsBeforeFilter

      if (verbose && groupFilter.length > 0) {
        println("")
        println("Groups after supplied filter:")
        println("----------------------------------------")
        if (allGroups.length <= 0)
          println("No Groups")
        else allGroups.foreach(println(_))
        println("----------------------------------------")
      }

      groups
    }

    // private def printGroupStates(groupsAndStates: List[(String, String)]): Unit = {
    //   // find proper columns width
    //   var maxGroupLen = 15
    //   for ((groupId, _) <- groupsAndStates) {
    //     maxGroupLen = Math.max(maxGroupLen, groupId.length)
    //   }
    //   println(s"%${-maxGroupLen}s %s".format("GROUP", "STATE"))
    //   for ((groupId, state) <- groupsAndStates) {
    //     println(s"%${-maxGroupLen}s %s".format(groupId, state))
    //   }
    // }

    // private def shouldPrintMemberState(group: String, state: Option[String], numRows: Option[Int]): Boolean = {
    //   // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
    //   // if it's undefined or 0, there is no relevant group information to display.
    //   numRows match {
    //     case None =>
    //       printError(s"The consumer group '$group' does not exist.")
    //       false
    //     case Some(num) => state match {
    //       case Some("Dead") =>
    //         printError(s"Consumer group '$group' does not exist.")
    //       case Some("Empty") =>
    //         Console.err.println(s"\nConsumer group '$group' has no active members.")
    //       case Some("PreparingRebalance") | Some("CompletingRebalance") =>
    //         Console.err.println(s"\nWarning: Consumer group '$group' is rebalancing.")
    //       case Some("Stable") =>
    //       case other =>
    //         // the control should never reach here
    //         throw new KafkaException(s"Expected a valid consumer group state, but found '${other.getOrElse("NONE")}'.")
    //     }
    //     !state.contains("Dead") && num > 0
    //   }
    // }

    // private def size(colOpt: Option[Seq[Object]]): Option[Int] = colOpt.map(_.size)

    // private def printOffsets(offsets: Map[String, (Option[String], Option[Seq[PartitionAssignmentState]])]): Unit = {
    //   for ((groupId, (state, assignments)) <- offsets) {
    //     if (shouldPrintMemberState(groupId, state, size(assignments))) {
    //       // find proper columns width
    //       var (maxGroupLen, maxTopicLen, maxConsumerIdLen, maxHostLen) = (15, 15, 15, 15)
    //       assignments match {
    //         case None => // do nothing
    //         case Some(consumerAssignments) =>
    //           consumerAssignments.foreach { consumerAssignment =>
    //             maxGroupLen = Math.max(maxGroupLen, consumerAssignment.group.length)
    //             maxTopicLen = Math.max(maxTopicLen, consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE).length)
    //             maxConsumerIdLen = Math.max(maxConsumerIdLen, consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE).length)
    //             maxHostLen = Math.max(maxHostLen, consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE).length)
    //           }
    //       }

    //       println(s"\n%${-maxGroupLen}s %${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s %${-maxHostLen}s %s"
    //         .format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"))

    //       assignments match {
    //         case None => // do nothing
    //         case Some(consumerAssignments) =>
    //           consumerAssignments.foreach { consumerAssignment =>
    //             println(s"%${-maxGroupLen}s %${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s %${-maxHostLen}s %s".format(
    //               consumerAssignment.group,
    //               consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.getOrElse(MISSING_COLUMN_VALUE),
    //               consumerAssignment.offset.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.getOrElse(MISSING_COLUMN_VALUE),
    //               consumerAssignment.lag.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE),
    //               consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.getOrElse(MISSING_COLUMN_VALUE))
    //             )
    //           }
    //       }
    //     }
    //   }
    // }

    // private def printMembers(members: Map[String, (Option[String], Option[Seq[MemberAssignmentState]])], verbose: Boolean): Unit = {
    //   for ((groupId, (state, assignments)) <- members) {
    //     if (shouldPrintMemberState(groupId, state, size(assignments))) {
    //       // find proper columns width
    //       var (maxGroupLen, maxConsumerIdLen, maxGroupInstanceIdLen, maxHostLen, maxClientIdLen, includeGroupInstanceId) = (15, 15, 17, 15, 15, false)
    //       assignments match {
    //         case None => // do nothing
    //         case Some(memberAssignments) =>
    //           memberAssignments.foreach { memberAssignment =>
    //             maxGroupLen = Math.max(maxGroupLen, memberAssignment.group.length)
    //             maxConsumerIdLen = Math.max(maxConsumerIdLen, memberAssignment.consumerId.length)
    //             maxGroupInstanceIdLen =  Math.max(maxGroupInstanceIdLen, memberAssignment.groupInstanceId.length)
    //             maxHostLen = Math.max(maxHostLen, memberAssignment.host.length)
    //             maxClientIdLen = Math.max(maxClientIdLen, memberAssignment.clientId.length)
    //             includeGroupInstanceId = includeGroupInstanceId || memberAssignment.groupInstanceId.nonEmpty
    //           }
    //       }

    //       if (includeGroupInstanceId) {
    //         print(s"\n%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxGroupInstanceIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "
    //             .format("GROUP", "CONSUMER-ID", "GROUP-INSTANCE-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
    //       } else {
    //         print(s"\n%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "
    //             .format("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
    //       }
    //       if (verbose)
    //         print(s"%s".format("ASSIGNMENT"))
    //       println()

    //       assignments match {
    //         case None => // do nothing
    //         case Some(memberAssignments) =>
    //           memberAssignments.foreach { memberAssignment =>
    //             if (includeGroupInstanceId) {
    //               print(s"%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxGroupInstanceIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s ".format(
    //                 memberAssignment.group, memberAssignment.consumerId, memberAssignment.groupInstanceId, memberAssignment.host,
    //                 memberAssignment.clientId, memberAssignment.numPartitions))
    //             } else {
    //               print(s"%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s ".format(
    //                 memberAssignment.group, memberAssignment.consumerId, memberAssignment.host, memberAssignment.clientId, memberAssignment.numPartitions))
    //             }
    //             if (verbose) {
    //               val partitions = memberAssignment.assignment match {
    //                 case List() => MISSING_COLUMN_VALUE
    //                 case assignment =>
    //                   assignment.groupBy(_.topic).map {
    //                     case (topic, partitionList) => topic + partitionList.map(_.partition).sorted.mkString("(", ",", ")")
    //                   }.toList.sorted.mkString(", ")
    //               }
    //               print(s"%s".format(partitions))
    //             }
    //             println()
    //           }
    //       }
    //     }
    //   }
    // }

    // private def printStates(states: Map[String, GroupState]): Unit = {
    //   for ((groupId, state) <- states) {
    //     if (shouldPrintMemberState(groupId, Some(state.state), Some(1))) {
    //       val coordinator = s"${state.coordinator.host}:${state.coordinator.port} (${state.coordinator.idString})"
    //       val coordinatorColLen = Math.max(25, coordinator.length)
    //       print(s"\n%${-coordinatorColLen}s %-25s %-20s %-15s %s".format("GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS"))
    //       print(s"\n%${-coordinatorColLen}s %-25s %-20s %-15s %s".format(state.group, coordinator, state.assignmentStrategy, state.state, state.numMembers))
    //       println()
    //     }
    //   }
    // }

    def describeGroups(): Unit = {
      // val groupIds =
      //   if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
      //   else opts.options.valuesOf(opts.groupOpt).asScala
      // val membersOptPresent = opts.options.has(opts.membersOpt)
      // val stateOptPresent = opts.options.has(opts.stateOpt)
      // val offsetsOptPresent = opts.options.has(opts.offsetsOpt)
      // val subActions = Seq(membersOptPresent, offsetsOptPresent, stateOptPresent).count(_ == true)

      // if (subActions == 0 || offsetsOptPresent) {
      //   val offsets = collectGroupsOffsets(groupIds)
      //   printOffsets(offsets)
      // } else if (membersOptPresent) {
      //   val members = collectGroupsMembers(groupIds, opts.options.has(opts.verboseOpt))
      //   printMembers(members, opts.options.has(opts.verboseOpt))
      // } else {
      //   val states = collectGroupsState(groupIds)
      //   printStates(states)
      // }
    }

    private def collectConsumerAssignment(group: String,
                                          coordinator: Option[Node],
                                          topicPartitions: Seq[TopicPartition],
                                          getPartitionOffset: TopicPartition => Option[Long],
                                          consumerIdOpt: Option[String],
                                          hostOpt: Option[String],
                                          clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {
      if (topicPartitions.isEmpty) {
        Array[PartitionAssignmentState](
          PartitionAssignmentState(group, coordinator, None, None, None, getLag(None, None), consumerIdOpt, hostOpt, clientIdOpt, None)
        )
      }
      else
        describePartitions(group, coordinator, topicPartitions.sortBy(_.partition), getPartitionOffset, consumerIdOpt, hostOpt, clientIdOpt)
    }

    private def getLag(offset: Option[Long], logEndOffset: Option[Long]): Option[Long] =
      offset.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))

    private def describePartitions(group: String,
                                   coordinator: Option[Node],
                                   topicPartitions: Seq[TopicPartition],
                                   getPartitionOffset: TopicPartition => Option[Long],
                                   consumerIdOpt: Option[String],
                                   hostOpt: Option[String],
                                   clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {

      def getDescribePartitionResult(topicPartition: TopicPartition, logEndOffsetOpt: Option[Long]): PartitionAssignmentState = {
        val offset = getPartitionOffset(topicPartition)
        PartitionAssignmentState(group, coordinator, Option(topicPartition.topic), Option(topicPartition.partition), offset,
          getLag(offset, logEndOffsetOpt), consumerIdOpt, hostOpt, clientIdOpt, logEndOffsetOpt)
      }

      getLogEndOffsets(group, topicPartitions).map {
        logEndOffsetResult =>
          logEndOffsetResult._2 match {
            case LogOffsetResult.LogOffset(logEndOffset) => getDescribePartitionResult(logEndOffsetResult._1, Some(logEndOffset))
            case LogOffsetResult.Unknown => getDescribePartitionResult(logEndOffsetResult._1, None)
            case LogOffsetResult.Ignore => null
          }
      }.toArray
    }

    // def resetOffsets(): Map[String, Map[TopicPartition, OffsetAndMetadata]] = {
    //   val groupIds =
    //     if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
    //     else opts.options.valuesOf(opts.groupOpt).asScala

    //   val consumerGroups = adminClient.describeConsumerGroups(
    //     groupIds.asJava,
    //     withTimeoutMs(new DescribeConsumerGroupsOptions)
    //   ).describedGroups()

    //   val result =
    //     consumerGroups.asScala.foldLeft(immutable.Map[String, Map[TopicPartition, OffsetAndMetadata]]()) {
    //       case (acc, (groupId, groupDescription)) =>
    //         groupDescription.get.state().toString match {
    //           case "Empty" | "Dead" =>
    //             val partitionsToReset = getPartitionsToReset(groupId)
    //             val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)

    //             // Dry-run is the default behavior if --execute is not specified
    //             val dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt)
    //             if (!dryRun) {
    //               adminClient.alterConsumerGroupOffsets(
    //                 groupId,
    //                 preparedOffsets.asJava,
    //                 withTimeoutMs(new AlterConsumerGroupOffsetsOptions)
    //               ).all.get
    //             }
    //             acc.updated(groupId, preparedOffsets)
    //           case currentState =>
    //             printError(s"Assignments can only be reset if the group '$groupId' is inactive, but the current state is $currentState.")
    //             acc.updated(groupId, Map.empty)
    //         }
    //     }
    //   result
    // }


    def deleteOffsets(groupId: String, topics: List[String]): (Errors, Map[TopicPartition, Throwable]) = {
      val partitionLevelResult = mutable.Map[TopicPartition, Throwable]()

      val (topicWithPartitions, topicWithoutPartitions) = topics.partition(_.contains(":"))
      val knownPartitions = topicWithPartitions.flatMap(parseTopicsWithPartitions)

      // Get the partitions of topics that the user did not explicitly specify the partitions
      val describeTopicsResult = adminClient.describeTopics(
        topicWithoutPartitions.asJava,
        withTimeoutMs(new DescribeTopicsOptions))

      val unknownPartitions = describeTopicsResult.topicNameValues().asScala.flatMap { case (topic, future) =>
        Try(future.get()) match {
          case Success(description) => description.partitions().asScala.map { partition =>
            new TopicPartition(topic, partition.partition())
          }
          case Failure(e) =>
            partitionLevelResult += new TopicPartition(topic, -1) -> e
            List.empty
        }
      }

      val partitions = knownPartitions ++ unknownPartitions

      val deleteResult = adminClient.deleteConsumerGroupOffsets(
        groupId,
        partitions.toSet.asJava,
        withTimeoutMs(new DeleteConsumerGroupOffsetsOptions)
      )

      var topLevelException = Errors.NONE
      Try(deleteResult.all.get) match {
        case Success(_) =>
        case Failure(e) => topLevelException = Errors.forException(e.getCause)
      }

      partitions.foreach { partition =>
        Try(deleteResult.partitionResult(partition).get()) match {
          case Success(_) => partitionLevelResult += partition -> null
          case Failure(e) => partitionLevelResult += partition -> e
        }
      }

      (topLevelException, partitionLevelResult)
    }

    def deleteOffsets(): Unit = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val topics = opts.options.valuesOf(opts.topicOpt).asScala.toList

      val (topLevelResult, partitionLevelResult) = deleteOffsets(groupId, topics)

      topLevelResult match {
        case Errors.NONE =>
          println(s"Request succeed for deleting offsets with topic ${topics.mkString(", ")} group $groupId")
        case Errors.INVALID_GROUP_ID =>
          printError(s"'$groupId' is not valid.")
        case Errors.GROUP_ID_NOT_FOUND =>
          printError(s"'$groupId' does not exist.")
        case Errors.GROUP_AUTHORIZATION_FAILED =>
          printError(s"Access to '$groupId' is not authorized.")
        case Errors.NON_EMPTY_GROUP =>
          printError(s"Deleting offsets of a consumer group '$groupId' is forbidden if the group is not empty.")
        case Errors.GROUP_SUBSCRIBED_TO_TOPIC |
             Errors.TOPIC_AUTHORIZATION_FAILED |
             Errors.UNKNOWN_TOPIC_OR_PARTITION =>
          printError(s"Encounter some partition level error, see the follow-up details:")
        case _ =>
          printError(s"Encounter some unknown error: $topLevelResult")
      }

      println("\n%-30s %-15s %-15s".format("TOPIC", "PARTITION", "STATUS"))
      partitionLevelResult.toList.sortBy(t => t._1.topic + t._1.partition.toString).foreach { case (tp, error) =>
        println("%-30s %-15s %-15s".format(
          tp.topic,
          if (tp.partition >= 0) tp.partition else "Not Provided",
          if (error != null) s"Error: ${error.getMessage}" else "Successful"
        ))
      }
    }

    private[admin] def describeConsumerGroups(groupIds: Seq[String]): mutable.Map[String, ConsumerGroupDescription] = {
      adminClient.describeConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups().asScala.map {
        case (groupId, groupDescriptionFuture) => (groupId, groupDescriptionFuture.get())
      }
    }

    /**
      * Returns the state of the specified consumer group and partition assignment states
      */
    // def collectGroupOffsets(groupId: String): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
    //   collectGroupsOffsets(List(groupId)).getOrElse(groupId, (None, None))
    // }

    /**
      * Returns states of the specified consumer groups and partition assignment states
      */
    def collectGroupsOffsets(groupIds: Seq[String]): TreeMap[String, (Option[String], Option[Seq[PartitionAssignmentState]])] = {
      val consumerGroups = describeConsumerGroups(groupIds)

      val groupOffsets = TreeMap[String, (Option[String], Option[Seq[PartitionAssignmentState]])]() ++ (for ((groupId, consumerGroup) <- consumerGroups) yield {
        val state = consumerGroup.state
        val committedOffsets = getCommittedOffsets(groupId)
        // The admin client returns `null` as a value to indicate that there is not committed offset for a partition.
        def getPartitionOffset(tp: TopicPartition): Option[Long] = committedOffsets.get(tp).filter(_ != null).map(_.offset)
        var assignedTopicPartitions = ListBuffer[TopicPartition]()
        val rowsWithConsumer = consumerGroup.members.asScala.filterNot(_.assignment.topicPartitions.isEmpty).toSeq
          .sortBy(_.assignment.topicPartitions.size)(Ordering[Int].reverse).flatMap { consumerSummary =>
          val topicPartitions = consumerSummary.assignment.topicPartitions.asScala
          assignedTopicPartitions = assignedTopicPartitions ++ topicPartitions
          collectConsumerAssignment(groupId, Option(consumerGroup.coordinator), topicPartitions.toList,
            getPartitionOffset, Some(s"${consumerSummary.consumerId}"), Some(s"${consumerSummary.host}"),
            Some(s"${consumerSummary.clientId}"))
        }
        val unassignedPartitions = committedOffsets.filterNot { case (tp, _) => assignedTopicPartitions.contains(tp) }
        val rowsWithoutConsumer = if (unassignedPartitions.nonEmpty) {
          collectConsumerAssignment(
            groupId,
            Option(consumerGroup.coordinator),
            unassignedPartitions.keySet.toSeq,
            getPartitionOffset,
            Some(MISSING_COLUMN_VALUE),
            Some(MISSING_COLUMN_VALUE),
            Some(MISSING_COLUMN_VALUE)).toSeq
        } else
          Seq.empty

        groupId -> (Some(state.toString), Some(rowsWithConsumer ++ rowsWithoutConsumer))
      }).toMap

      groupOffsets
    }

    private[admin] def collectGroupMembers(groupId: String, verbose: Boolean): (Option[String], Option[Seq[MemberAssignmentState]]) = {
      collectGroupsMembers(Seq(groupId), verbose)(groupId)
    }

    private[admin] def collectGroupsMembers(groupIds: Seq[String], verbose: Boolean): TreeMap[String, (Option[String], Option[Seq[MemberAssignmentState]])] = {
      val consumerGroups = describeConsumerGroups(groupIds)
      TreeMap[String, (Option[String], Option[Seq[MemberAssignmentState]])]() ++ (for ((groupId, consumerGroup) <- consumerGroups) yield {
        val state = consumerGroup.state.toString
        val memberAssignmentStates = consumerGroup.members().asScala.map(consumer =>
          MemberAssignmentState(
            groupId,
            consumer.consumerId,
            consumer.host,
            consumer.clientId,
            consumer.groupInstanceId.orElse(""),
            consumer.assignment.topicPartitions.size(),
            if (verbose) consumer.assignment.topicPartitions.asScala.toList else List()
          )).toList
        groupId -> (Some(state), Option(memberAssignmentStates))
      }).toMap
    }

    private[admin] def collectGroupState(groupId: String): GroupState = {
      collectGroupsState(Seq(groupId))(groupId)
    }

    private[admin] def collectGroupsState(groupIds: Seq[String]): TreeMap[String, GroupState] = {
      val consumerGroups = describeConsumerGroups(groupIds)
      TreeMap[String, GroupState]() ++ (for ((groupId, groupDescription) <- consumerGroups) yield {
        groupId -> GroupState(
          groupId,
          groupDescription.coordinator,
          groupDescription.partitionAssignor(),
          groupDescription.state.toString,
          groupDescription.members().size
        )
      }).toMap
    }

    private def getLogEndOffsets(groupId: String, topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val endOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.latest
      }.toMap
      val offsets = adminClient.listOffsets(
        endOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    

    // private def getLogStartOffsets(groupId: String, topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
    //   val startOffsets = topicPartitions.map { topicPartition =>
    //     topicPartition -> OffsetSpec.earliest
    //   }.toMap
    //   val offsets = adminClient.listOffsets(
    //     startOffsets.asJava,
    //     withTimeoutMs(new ListOffsetsOptions)
    //   ).all.get
    //   topicPartitions.map { topicPartition =>
    //     Option(offsets.get(topicPartition)) match {
    //       case Some(listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
    //       case _ => topicPartition -> LogOffsetResult.Unknown
    //     }
    //   }.toMap
    // }

    // private def getLogTimestampOffsets(groupId: String, topicPartitions: Seq[TopicPartition], timestamp: java.lang.Long): Map[TopicPartition, LogOffsetResult] = {
    //   val timestampOffsets = topicPartitions.map { topicPartition =>
    //     topicPartition -> OffsetSpec.forTimestamp(timestamp)
    //   }.toMap
    //   val offsets = adminClient.listOffsets(
    //     timestampOffsets.asJava,
    //     withTimeoutMs(new ListOffsetsOptions)
    //   ).all.get
    //   val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
    //     offsets.asScala.partition(_._2.offset != ListOffsetsResponse.UNKNOWN_OFFSET)

    //   val successfulLogTimestampOffsets = successfulOffsetsForTimes.map {
    //     case (topicPartition, listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
    //   }.toMap

    //   unsuccessfulOffsetsForTimes.foreach { entry =>
    //     println(s"\nWarn: Partition " + entry._1.partition() + " from topic " + entry._1.topic() +
    //       " is empty. Falling back to latest known offset.")
    //   }

    //   successfulLogTimestampOffsets ++ getLogEndOffsets(groupId, unsuccessfulOffsetsForTimes.keySet.toSeq)
    // }

    def getTopicRetentionTime(topic:String): Long = {
      var retentionMs = 0L
      val configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val configs = adminClient.describeConfigs(Collections.singleton(configResource)).all.get(30, TimeUnit.SECONDS)
      val retentionMsConfig = configs.get(configResource).entries().asScala
                         .filter(item => "retention.ms".equals(item.name))
                         .map( x => {
                          val value = x.value().toLong
                          if (value > 0L) {
                            value
                          }
                         })

      retentionMs = retentionMsConfig.head.asInstanceOf[Number].longValue

      retentionMs
    }

    def setTopicRetentionTime(topicRetention: HashMap[String, HashMap[String, Long]]): Unit = {

      val configs = new HashMap[ConfigResource, Collection[AlterConfigOp]]()
      topicRetention.foreach{ case (topic, details) =>
        val ops = new ArrayList[AlterConfigOp]
        val proposed = details.get("proposed").getOrElse(-1L)
        if (proposed > 0L) {
          println(f"Setting retention for $topic to $proposed.")
          ops.add(new AlterConfigOp(new ConfigEntry("retention.ms", proposed.toString), OpType.SET))
          configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), ops)
        } else {
          println(f"Skipping setting retention for $topic due to a bad proposed value.")
        }
      }
      

      adminClient.incrementalAlterConfigs(configs.asJava).all().get()

    }

    def close(): Unit = {
      adminClient.close()
    }

    // Visibility for testing
    protected def createAdminClient(configOverrides: Map[String, String]): Admin = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      configOverrides.forKeyValue { (k, v) => props.put(k, v)}
      Admin.create(props)
    }

    private def withTimeoutMs [T <: AbstractOptions[T]] (options : T) =  {
      val t = opts.options.valueOf(opts.timeoutMsOpt).intValue()
      options.timeoutMs(t)
    }

    private def parseTopicsWithPartitions(topicArg: String): Seq[TopicPartition] = {
      def partitionNum(partition: String): Int = {
        try {
          partition.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"Invalid partition '$partition' specified in topic arg '$topicArg''")
        }
      }
      topicArg.split(":") match {
        case Array(topic, partitions) =>
          partitions.split(",").map(partition => new TopicPartition(topic, partitionNum(partition)))
        case _ =>
          throw new IllegalArgumentException(s"Invalid topic arg '$topicArg', expected topic name and partitions")
      }
    }

    // private def parseTopicPartitionsToReset(topicArgs: Seq[String]): Seq[TopicPartition] = {
    //   val (topicsWithPartitions, topics) = topicArgs.partition(_.contains(":"))
    //   val specifiedPartitions = topicsWithPartitions.flatMap(parseTopicsWithPartitions)

    //   val unspecifiedPartitions = if (topics.nonEmpty) {
    //     val descriptionMap = adminClient.describeTopics(
    //       topics.asJava,
    //       withTimeoutMs(new DescribeTopicsOptions)
    //     ).allTopicNames().get.asScala
    //     descriptionMap.flatMap { case (topic, description) =>
    //       description.partitions().asScala.map { tpInfo =>
    //         new TopicPartition(topic, tpInfo.partition)
    //       }
    //     }
    //   } else
    //     Seq.empty
    //   specifiedPartitions ++ unspecifiedPartitions
    // }

    // private def getPartitionsToReset(groupId: String): Seq[TopicPartition] = {
    //   if (opts.options.has(opts.allTopicsOpt)) {
    //     getCommittedOffsets(groupId).keys.toSeq
    //   } else if (opts.options.has(opts.topicOpt)) {
    //     val topics = opts.options.valuesOf(opts.topicOpt).asScala
    //     parseTopicPartitionsToReset(topics)
    //   } else {
    //     if (opts.options.has(opts.resetFromFileOpt))
    //       Nil
    //     else
    //       ToolsUtils.printUsageAndExit(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.")
    //   }
    // }

    private def getCommittedOffsets(groupId: String): Map[TopicPartition, OffsetAndMetadata] = {
      adminClient.listConsumerGroupOffsets(
        Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec),
        withTimeoutMs(new ListConsumerGroupOffsetsOptions())
      ).partitionsToOffsetAndMetadata(groupId).get().asScala
    }

    // type GroupMetadata = immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]
    // private def parseResetPlan(resetPlanCsv: String): GroupMetadata = {
    //   def updateGroupMetadata(group: String, topic: String, partition: Int, offset: Long, acc: GroupMetadata) = {
    //     val topicPartition = new TopicPartition(topic, partition)
    //     val offsetAndMetadata = new OffsetAndMetadata(offset)
    //     val dataMap = acc.getOrElse(group, immutable.Map()).updated(topicPartition, offsetAndMetadata)
    //     acc.updated(group, dataMap)
    //   }
    //   val csvReader = CsvUtils().readerFor[CsvRecordNoGroup]
    //   val lines = resetPlanCsv.split("\n")
    //   val isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1
    //   val isOldCsvFormat = lines.headOption.flatMap(line =>
    //     Try(csvReader.readValue[CsvRecordNoGroup](line)).toOption).nonEmpty
    //   // Single group CSV format: "topic,partition,offset"
    //   val dataMap = if (isSingleGroupQuery && isOldCsvFormat) {
    //     val group = opts.options.valueOf(opts.groupOpt)
    //     lines.foldLeft(immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]()) { (acc, line) =>
    //       val CsvRecordNoGroup(topic, partition, offset) = csvReader.readValue[CsvRecordNoGroup](line)
    //       updateGroupMetadata(group, topic, partition, offset, acc)
    //     }
    //     // Multiple group CSV format: "group,topic,partition,offset"
    //   } else {
    //     val csvReader = CsvUtils().readerFor[CsvRecordWithGroup]
    //     lines.foldLeft(immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]()) { (acc, line) =>
    //       val CsvRecordWithGroup(group, topic, partition, offset) = csvReader.readValue[CsvRecordWithGroup](line)
    //       updateGroupMetadata(group, topic, partition, offset, acc)
    //     }
    //   }
    //   dataMap
    // }

    // private def prepareOffsetsToReset(groupId: String,
    //                                   partitionsToReset: Seq[TopicPartition]): Map[TopicPartition, OffsetAndMetadata] = {
    //   if (opts.options.has(opts.resetToOffsetOpt)) {
    //     val offset = opts.options.valueOf(opts.resetToOffsetOpt)
    //     checkOffsetsRange(groupId, partitionsToReset.map((_, offset)).toMap).map {
    //       case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
    //     }
    //   } else if (opts.options.has(opts.resetToEarliestOpt)) {
    //     val logStartOffsets = getLogStartOffsets(groupId, partitionsToReset)
    //     partitionsToReset.map { topicPartition =>
    //       logStartOffsets.get(topicPartition) match {
    //         case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
    //         case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting starting offset of topic partition: $topicPartition")
    //       }
    //     }.toMap
    //   } else if (opts.options.has(opts.resetToLatestOpt)) {
    //     val logEndOffsets = getLogEndOffsets(groupId, partitionsToReset)
    //     partitionsToReset.map { topicPartition =>
    //       logEndOffsets.get(topicPartition) match {
    //         case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
    //         case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
    //       }
    //     }.toMap
    //   } else if (opts.options.has(opts.resetShiftByOpt)) {
    //     val currentCommittedOffsets = getCommittedOffsets(groupId)
    //     val requestedOffsets = partitionsToReset.map { topicPartition =>
    //       val shiftBy = opts.options.valueOf(opts.resetShiftByOpt)
    //       val currentOffset = currentCommittedOffsets.getOrElse(topicPartition,
    //         throw new IllegalArgumentException(s"Cannot shift offset for partition $topicPartition since there is no current committed offset")).offset
    //       (topicPartition, currentOffset + shiftBy)
    //     }.toMap
    //     checkOffsetsRange(groupId, requestedOffsets).map {
    //       case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
    //     }
    //   } else if (opts.options.has(opts.resetToDatetimeOpt)) {
    //     val timestamp = Utils.getDateTime(opts.options.valueOf(opts.resetToDatetimeOpt))
    //     val logTimestampOffsets = getLogTimestampOffsets(groupId, partitionsToReset, timestamp)
    //     partitionsToReset.map { topicPartition =>
    //       val logTimestampOffset = logTimestampOffsets.get(topicPartition)
    //       logTimestampOffset match {
    //         case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
    //         case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
    //       }
    //     }.toMap
    //   } else if (opts.options.has(opts.resetByDurationOpt)) {
    //     val duration = opts.options.valueOf(opts.resetByDurationOpt)
    //     val durationParsed = Duration.parse(duration)
    //     val now = Instant.now()
    //     durationParsed.negated().addTo(now)
    //     val timestamp = now.minus(durationParsed).toEpochMilli
    //     val logTimestampOffsets = getLogTimestampOffsets(groupId, partitionsToReset, timestamp)
    //     partitionsToReset.map { topicPartition =>
    //       val logTimestampOffset = logTimestampOffsets.get(topicPartition)
    //       logTimestampOffset match {
    //         case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
    //         case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
    //       }
    //     }.toMap
    //   } else if (resetPlanFromFile.isDefined) {
    //     resetPlanFromFile.map(resetPlan => resetPlan.get(groupId).map { resetPlanForGroup =>
    //       val requestedOffsets = resetPlanForGroup.keySet.map { topicPartition =>
    //         topicPartition -> resetPlanForGroup(topicPartition).offset
    //       }.toMap
    //       checkOffsetsRange(groupId, requestedOffsets).map {
    //         case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
    //       }
    //     } match {
    //       case Some(resetPlanForGroup) => resetPlanForGroup
    //       case None =>
    //         printError(s"No reset plan for group $groupId found")
    //         Map[TopicPartition, OffsetAndMetadata]()
    //     }).getOrElse(Map.empty)
    //   } else if (opts.options.has(opts.resetToCurrentOpt)) {
    //     val currentCommittedOffsets = getCommittedOffsets(groupId)
    //     val (partitionsToResetWithCommittedOffset, partitionsToResetWithoutCommittedOffset) =
    //       partitionsToReset.partition(currentCommittedOffsets.keySet.contains(_))

    //     val preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.map { topicPartition =>
    //       (topicPartition, new OffsetAndMetadata(currentCommittedOffsets.get(topicPartition) match {
    //         case Some(offset) => offset.offset
    //         case None => throw new IllegalStateException(s"Expected a valid current offset for topic partition: $topicPartition")
    //       }))
    //     }.toMap

    //     val preparedOffsetsForPartitionsWithoutCommittedOffset = getLogEndOffsets(groupId, partitionsToResetWithoutCommittedOffset).map {
    //       case (topicPartition, LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
    //       case (topicPartition, _) => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
    //     }

    //     preparedOffsetsForPartitionsWithCommittedOffset ++ preparedOffsetsForPartitionsWithoutCommittedOffset
    //   } else {
    //     ToolsUtils.printUsageAndExit(opts.parser, "Option '%s' requires one of the following scenarios: %s".format(opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts))
    //   }
    // }

    // private def checkOffsetsRange(groupId: String, requestedOffsets: Map[TopicPartition, Long]) = {
    //   val logStartOffsets = getLogStartOffsets(groupId, requestedOffsets.keySet.toSeq)
    //   val logEndOffsets = getLogEndOffsets(groupId, requestedOffsets.keySet.toSeq)
    //   requestedOffsets.map { case (topicPartition, offset) => (topicPartition,
    //     logEndOffsets.get(topicPartition) match {
    //       case Some(LogOffsetResult.LogOffset(endOffset)) if offset > endOffset =>
    //         warn(s"New offset ($offset) is higher than latest offset for topic partition $topicPartition. Value will be set to $endOffset")
    //         endOffset

    //       case Some(_) => logStartOffsets.get(topicPartition) match {
    //         case Some(LogOffsetResult.LogOffset(startOffset)) if offset < startOffset =>
    //           warn(s"New offset ($offset) is lower than earliest offset for topic partition $topicPartition. Value will be set to $startOffset")
    //           startOffset

    //         case _ => offset
    //       }

    //       case None => // the control should not reach here
    //         throw new IllegalStateException(s"Unexpected non-existing offset value for topic partition $topicPartition")
    //     })
    //   }
    // }

    def deleteGroups(): Map[String, Throwable] = {
      val groupIds =
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
        else opts.options.valuesOf(opts.groupOpt).asScala

      val groupsToDelete = adminClient.deleteConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DeleteConsumerGroupsOptions)
      ).deletedGroups().asScala

      val result = groupsToDelete.map { case (g, f) =>
        Try(f.get) match {
          case Success(_) => g -> null
          case Failure(e) => g -> e
        }
      }

      val (success, failed) = result.partition {
        case (_, error) => error == null
      }

      if (failed.isEmpty) {
        println(s"Deletion of requested consumer groups (${success.keySet.mkString("'", "', '", "'")}) was successful.")
      }
      else {
        printError("Deletion of some consumer groups failed:")
        failed.foreach {
          case (group, error) => println(s"* Group '$group' could not be deleted due to: ${error.toString}")
        }
        if (success.nonEmpty)
          println(s"\nThese consumer groups were deleted successfully: ${success.keySet.mkString("'", "', '", "'")}")
      }

      result.toMap
    }
  }



  sealed trait LogOffsetResult

  object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  class RefactorRetentionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val BootstrapServerDoc = "REQUIRED: The server(s) to connect to."
    val TimeoutMsDoc = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
      "to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, " +
      "or is going through some changes)."
    val GroupDoc = "The consumer group(s) we wish to act on. " +
                   "Example: --group enrich-*,route-*,ext-*"
    val AllGroupsDoc = "All consumer group(s) besides internal, ksql, and connect will be included."
    val TopicDoc = "The specific topic(s) we want to apply the changes to."
    val AllTopicsDoc = "Consider all topics assigned to a group in the `refactor` process."
    val nl = System.getProperty("line.separator")
    val RetentionMsDoc = "Specific retention (milliseconds) time for a topic (default = 4 days)"
    val RetentionMinMsDoc = "Minimum retention (milliseconds) time for a topic (default = 1 hour)"
    val RetentionMaxMsDoc = "Maximum retention (milliseconds) time for a topic (default = 7 days)"
    val RetentionStepMsDoc = "Amount of (milliseconds) time to reduce/increase a topic by (default = 1 hour)"
    val CommandConfigDoc = "Property file containing configs to be passed to Admin Client and Consumer."
    val ExecuteDoc = "Execute operation. If the user does not `--execute` the plan will be exported, but nothing will actually change."
    val VerboseDoc = "Provide additional information, if any, when describing the group or calculating the retention times."

    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server to connect to")
                                   .ofType(classOf[String])
    val timeoutMsOpt = parser.accepts("timeout", TimeoutMsDoc)
                             .withRequiredArg
                             .describedAs("timeout (ms)")
                             .ofType(classOf[Long])
                             .defaultsTo(5000)
    val groupOpt = parser.accepts("group", GroupDoc)
                             .withRequiredArg
                             .describedAs("consumer group")
                             .ofType(classOf[String])
    val allGroupsOpt = parser.accepts("all-groups", AllGroupsDoc)
                                .withRequiredArg
                                .describedAs("consumer group")
                                .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", TopicDoc)
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val retentionMsOpt = parser.accepts("retention", RetentionMsDoc)
                               .withRequiredArg
                               .describedAs("timeout (ms)")
                               .ofType(classOf[Long])
    val retentionMinMsOpt = parser.accepts("retention-min", RetentionMinMsDoc)
                                  .withRequiredArg
                                  .describedAs("timeout (ms)")
                                  .ofType(classOf[Long])
    val retentionMaxMsOpt = parser.accepts("retention-max", RetentionMaxMsDoc)
                                  .withRequiredArg
                                  .describedAs("timeout (ms)")
                                  .ofType(classOf[Long])
    val retentionStepMsOpt = parser.accepts("retention-step", RetentionStepMsDoc)
                                   .withRequiredArg
                                   .describedAs("timeout (ms)")
                                   .ofType(classOf[Long])
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                 .withRequiredArg
                                 .describedAs("command config property file")
                                 .ofType(classOf[String])
    val executeOpt = parser.accepts("execute", ExecuteDoc)
    val verboseOpt = parser.accepts("verbose", VerboseDoc)

    options = parser.parse(args : _*)

    val allConsumerSelectionScopeOpts = immutable.Set[OptionSpec[_]](groupOpt, allGroupsOpt)
    val allRetentionOpts = immutable.Set[OptionSpec[_]](retentionMsOpt, retentionMinMsOpt, retentionMaxMsOpt, retentionStepMsOpt)

    def checkArgs(): Unit = {

      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (options.has(groupOpt) && options.has(allGroupsOpt)) {
        CommandLineUtils.printUsageAndExit(parser,
            s"Option $groupOpt may not be combined with $allGroupsOpt")
      }

      if (options.has(retentionMsOpt) && (options.has(retentionMinMsOpt) || options.has(retentionMaxMsOpt) || options.has(retentionStepMsOpt))) {
        val nonCompatibleOpts: Set[OptionSpec[_]] = Set(retentionMinMsOpt, retentionMaxMsOpt, retentionStepMsOpt)
        if (nonCompatibleOpts.toList.map(o => if (options.has(o)) 1 else 0).sum > 1) {
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $retentionMsOpt may not be combined with : ${nonCompatibleOpts.mkString(", ")}")
        }
      }

      if (!options.has(executeOpt)) {
          Console.err.println("WARN: No action will be performed as the --execute option is missing.")
      }

      // CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, (allGroupSelectionScopeOpts - groupOpt).asJava)
      // CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, (allConsumerGroupLevelOpts - describeOpt - deleteOpt - resetOffsetsOpt).asJava)
      // CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, (allConsumerGroupLevelOpts - deleteOpt - resetOffsetsOpt).asJava )
    }
  }
}
