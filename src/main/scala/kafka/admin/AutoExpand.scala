package kafka.admin

import java.util.concurrent.{CountDownLatch, LinkedBlockingDeque, TimeUnit}
import java.util.{Collections, List => JList}

import joptsimple.OptionParser
import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.utils._
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.CreateMode

import _root_.scala.collection.JavaConversions._
import scala.collection._


object AutoExpandCommand {
  val KAFKA_POD_MASTER = "/kafka-master-pod"
  val KAFKA_PODS_UID = "/kafka-pods-uid"
  val KAFKA_PODS_UID_GENERATOR = "/kafka-pods-uid-genreator"
  val GENERATE_MODE = "generate"
  class SearchableSeq[T](a: Seq[T])(implicit ordering: Ordering[T]) {
    val list: JList[T] = a.toList

    def binarySearch(key: T): Int = Collections.binarySearch(list, key, ordering)
  }

  implicit def seqToSearchable[T](a: Seq[T])(implicit ordering: Ordering[T]) =
    new SearchableSeq(a)(ordering)

  def main(args: Array[String]): Unit = {
    val opts = new AutoExpandCommandOptions(args)
    opts.options.valueOf(opts.mode) match{
      case GENERATE_MODE=>
        println(generate_broker(opts))
      case _=>
        monitor(opts)
    }
  }
  def monitor(opts: AutoExpandCommandOptions): Unit = {
    val uid = opts.options.valueOf(opts.selfBroker)
    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    zkUtils.makeSurePersistentPathExists(KAFKA_POD_MASTER)
    val mNode = zkUtils.zkClient.createEphemeralSequential(KAFKA_POD_MASTER+"/pod-", uid)
    val index = sequnce(mNode)
    case class Broker(index: Int, id: String) extends Ordered[Broker] {

      def compare(that: Broker): Int = this.index.compareTo(that.index)
    }
    while(true) {
      val children = zkUtils.zkClient.getChildren(KAFKA_POD_MASTER).map {
        s =>
          Broker(sequnce(s), s)
      }.sorted
      val aIndex = children.binarySearch(Broker(index, mNode))
      if (aIndex == 0) {
        leader_loop(mNode.replace(KAFKA_POD_MASTER + "/", ""), zkUtils)
      } else {
        val w = new CountDownLatch(1)
        new Thread(new Runnable {
          override def run(): Unit = {
            val prev = children.get(aIndex - 1)
            println("wath " + "/kafka-master-pod/" + prev.id)
            zkUtils.zkClient.subscribeDataChanges("/kafka-master-pod/" + prev.id, new IZkDataListener {
              override def handleDataChange(dataPath: String, data: scala.Any): Unit = {

              }
              override def handleDataDeleted(dataPath: String): Unit = {
                zkUtils.zkClient.unsubscribeAll()
                w.countDown()
              }
            })
          }
        }).start()
        w.await()
      }
    }
  }

  def generate_broker(opts: AutoExpandCommandOptions) = {
    val uid = opts.options.valueOf(opts.selfBroker)
    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    zkUtils.makeSurePersistentPathExists(KAFKA_PODS_UID)
    zkUtils.zkClient.readData[String](KAFKA_PODS_UID+"/"+uid,true) match {
      case id:String if id!=null=>
        Integer.parseInt(id)
      case _=>
        zkUtils.makeSurePersistentPathExists(KAFKA_PODS_UID_GENERATOR)
        val id = sequnce(zkUtils.zkClient.createEphemeralSequential(KAFKA_PODS_UID_GENERATOR+"/pod-",uid))+1
        zkUtils.zkClient.create(KAFKA_PODS_UID+"/"+uid,id.toString,CreateMode.PERSISTENT)
        id
    }
  }
  def sequnce(node: String) = {
    val index = node.lastIndexOf("-")
    if (index > 0) {
      Integer.parseInt(node.substring(index + 1))
    } else {
      -1
    }
  }

  def leader_loop(node: String,zkUtils: ZkUtils) = {
    println("Start leader: "+node)
    val extender = new Extender(zkUtils)
    new Thread(extender).start()
    val w = new CountDownLatch(1)
    new Thread(new Runnable {
      override def run(): Unit = {
        zkUtils.zkClient.subscribeChildChanges(KAFKA_POD_MASTER, new IZkChildListener {
          override def handleChildChange(parentPath: String, currentChilds: JList[String]): Unit = {
            if (!currentChilds.contains(node)){
              w.countDown()
            } else {
              extender.queue.add(currentChilds)
            }
          }
        })
      }
    }).start()
    extender.queue.add(zkUtils.zkClient.getChildren(KAFKA_POD_MASTER))
    w.await()
    System.exit(0)
  }

  class Extender(zkUtils: ZkUtils) extends Runnable {
    val queue = new LinkedBlockingDeque[JList[String]]()
    override def run(): Unit = {
      while(true){
        if(queue.poll(1,TimeUnit.MINUTES)!=null){
          while(queue.poll(10,TimeUnit.SECONDS)!=null){}
          println("Check expand")
          expand(zkUtils)
        }
      }
    }
  }

  /*
   * Slightly altered version of:
   *   https://github.com/apache/kafka/blob/0.10.0/core/src/main/scala/kafka/admin/ReassignPartitionsCommand.scala#L100
   * Changes:
   * - topicsToReassign is a sequence of strings instead of a json string
   */
  def generateAssignment(zkUtils: ZkUtils, brokerListToReassign: Seq[Int], topicsToReassign: Seq[String], disableRackAware: Boolean): (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]]) = {
    val duplicateTopicsToReassign = CoreUtils.duplicates(topicsToReassign)
    if (duplicateTopicsToReassign.nonEmpty)
      throw new AdminCommandFailedException("List of topics to reassign contains duplicate entries: %s".format(duplicateTopicsToReassign.mkString(",")))
    val currentAssignment = zkUtils.getReplicaAssignmentForTopics(topicsToReassign)

    val groupedByTopic = currentAssignment.groupBy { case (tp, _) => tp.topic }
    val rackAwareMode = if (disableRackAware) RackAwareMode.Disabled else RackAwareMode.Enforced
    val brokerMetadatas = AdminUtils.getBrokerMetadatas(zkUtils, rackAwareMode, Some(brokerListToReassign))

    val partitionsToBeReassigned = mutable.Map[TopicAndPartition, Seq[Int]]()
    groupedByTopic.foreach { case (topic, assignment) =>
      val (_, replicas) = assignment.head
      val assignedReplicas = AdminUtils.assignReplicasToBrokers(brokerMetadatas, assignment.size, replicas.size)
      partitionsToBeReassigned ++= assignedReplicas.map { case (partition, replicas) =>
        TopicAndPartition(topic, partition) -> replicas
      }
    }
    (partitionsToBeReassigned, currentAssignment)
  }

  def expand(zkUtils: ZkUtils): Unit = {
    val newBrokersIds = zkUtils.getAllBrokersInCluster().map{
      b => b.id
    }
    if (newBrokersIds.size<1){
      println("No broker found")
      return
    }
    val topics = zkUtils.getAllTopics()
    if (topics.size<1){
      println("No topics found")
      return
    }

    var (partitionsToBeReassigned, currentAssignments) = generateAssignment(zkUtils, newBrokersIds, topics, true)
    println("Current partition replica assignment\n\n%s".format(ZkUtils.formatAsReassignmentJson(currentAssignments)))
    println("Proposed partition reassignment configuration\n\n%s".format(ZkUtils.formatAsReassignmentJson(partitionsToBeReassigned)))
    val exec = new ReassignPartitionsCommand(zkUtils, partitionsToBeReassigned)
    if (exec.reassignPartitions()){
      var inProgress = true
      val start = System.currentTimeMillis()
      var stop = start
      var attemt = 1
      while (inProgress && (stop-start)<300000) {
        Thread.sleep(10000)
        println("Check reassigment attempt - %d".format(attemt))
        val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkUtils, partitionsToBeReassigned)
        partitionsToBeReassigned = partitionsToBeReassigned.filterKeys {
          partition =>
            reassignedPartitionsStatus.getOrElse(partition, ReassignmentInProgress) match {
              case ReassignmentCompleted =>
                println("Reassignment of partition %s completed successfully".format(partition))
                false
              case ReassignmentFailed =>
                println("Reassignment of partition %s failed".format(partition))
                false
              case ReassignmentInProgress =>
                println("Reassignment of partition %s is still in progress".format(partition))
                true
            }
        }
        inProgress = !partitionsToBeReassigned.isEmpty
        stop = System.currentTimeMillis()
        attemt+=1
      }
      if (inProgress){
        println("Timeout reassignment partitions:\n\n%s".format(ZkUtils.formatAsReassignmentJson(currentAssignments)))
      } else{
        println("Reassignment compleated!!!")
      }
    } else{
      println("Invalid reassignment data. Emty partitions found!!!")
    }
    println("Rebalancing DONE!!!")

  }
  private def checkIfReassignmentSucceeded(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
    partitionsToBeReassigned.map { topicAndPartition =>
      (topicAndPartition._1, checkIfPartitionReassignmentSucceeded(zkUtils,topicAndPartition._1,
        topicAndPartition._2, partitionsToBeReassigned, partitionsBeingReassigned))
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)
        if(assignedReplicas == newReplicas)
          ReassignmentCompleted
        else {
          println(("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
            " for partition %s").format(assignedReplicas.mkString(","), newReplicas.mkString(","), topicAndPartition))
          ReassignmentFailed
        }
    }
  }
}

class AutoExpandCommandOptions(args: Array[String]) {
  val parser = new OptionParser

  val zkConnectOpt = parser.accepts("zookeeper", "The connection string for the zookeeper connection in the " +
    "form host:port. Multiple URLS can be given to allow fail-over.")
    .withOptionalArg()
    .defaultsTo("localhost:2181")
    .describedAs("urls")
    .ofType(classOf[String])

  val selfBroker = parser.accepts("broker", "Self broker addr")
    .withOptionalArg().defaultsTo("").ofType(classOf[String])

  val mode = parser.accepts("mode", "Generate broker")
    .withOptionalArg()
    .defaultsTo("generate")
    .ofType(classOf[String])

  val options = parser.parse(args: _*)

}
