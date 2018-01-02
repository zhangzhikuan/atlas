package com.atlas

import java.util

import com.google.protobuf.ByteString
import org.apache.mesos.Protos.FrameworkInfo.Capability
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, Protos, Scheduler, SchedulerDriver}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * 自定义atlas调度器
  *
  * @param config          配置文件
  * @param checkpoint      备份地址
  * @param failoverTimeout 重试最长时间
  */
class AtlasScheduler(config: SchedulerConfig,
                     checkpoint: Option[Boolean] = None,
                     failoverTimeout: Option[Double] = Some(Int.MaxValue),
                     frameworkIDStore: FrameworkIDStore) extends Scheduler {

  //日志
  private val logger = LoggerFactory.getLogger(classOf[AtlasScheduler])
  //全局锁
  private val stateLock = new Object()

  //start of scheduler
  //注册成功监听
  override def registered(driver: SchedulerDriver, frameworkID: Protos.FrameworkID, masterInfo: Protos.MasterInfo): Unit = {
    logger.info("Registered as framework ID " + frameworkID.getValue)
    frameworkIDStore.storeFrameworkID(frameworkID.getValue)
  }

  //任务状态回调
  override def statusUpdate(schedulerDriver: SchedulerDriver, status: Protos.TaskStatus): Unit = {
    stateLock.synchronized {
      val taskId = status.getTaskId.getValue
      status.getState match {
        case TaskState.TASK_FINISHED =>
          logger.info(s"Received status update: taskId=$taskId state=${status.getState} message=${status.getMessage} data=${status.getData.toStringUtf8}")
        case TaskState.TASK_RUNNING =>
          logger.info(s"Received status update: taskId=$taskId state=${status.getState}")
        case _ =>
          logger.warn(s"Received status update: taskId=$taskId state=${status.getState} message=${status.getMessage} reason=${status.getReason}")
      }
    }
  }

  //错误回调
  override def error(schedulerDriver: SchedulerDriver, error: String): Unit = {
    logger.info("Error received: " + error)
  }

  val slaveIdToExecutorInfo = new mutable.HashMap[String, ExecutorInfo]()
  //  val executorToSlaveInfo = new mutable.HashMap[String, SlaveInfo]()

  //核心代码
  override def resourceOffers(schedulerDriver: SchedulerDriver,
                              offers: util.List[Protos.Offer]): Unit = {
    stateLock.synchronized {

      val tasks = offers.asScala.flatMap {
        offer =>
          //CPU信息
          val cpuResourcesToUse = Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(1).build()).build()

          //内存
          val memResourcesToUse = Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(256).build()).build()

          val slaveID: String = offer.getSlaveId.getValue
          val executorInfo = if (slaveIdToExecutorInfo.contains(slaveID)) {
            slaveIdToExecutorInfo(slaveID) //意味着一对一的关系
          } else {
            slaveIdToExecutorInfo.put(slaveID, createExecutorInfo(s"atlas_executor_$slaveID"))
            slaveIdToExecutorInfo(slaveID)
          }

          val taskInfo = TaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(s"taskId_${new DateTime().getMillis}"))
            .setSlaveId(offer.getSlaveId)
            .setExecutor(executorInfo)
            .setName(s"taskName_${new DateTime().getMillis}")
            .addResources(cpuResourcesToUse)
            .addResources(memResourcesToUse)

            .setData(ByteString.copyFromUtf8("ls /tmp"))
            .build()
          Iterator.single((offer, taskInfo))
      }
      Thread.sleep(1000)
      tasks.foreach {
        case (offer, task) =>
          schedulerDriver.launchTasks(util.Collections.singleton(offer.getId), util.Collections.singleton(task))
      }
      //offers.asScala.foreach(o => schedulerDriver.declineOffer(o.getId))
    }
  }

  //断开链接，等待重试
  override def disconnected(driver: SchedulerDriver): Unit = {
    logger.info(s"Framework driver disconnected")
  }

  //slave失效
  override def slaveLost(schedulerDriver: SchedulerDriver, slaveID: Protos.SlaveID): Unit = {
    logger.info(s"Framework slave lost on $slaveID")
    stateLock.synchronized {
      //删除slave到executor的映射关系
      slaveIdToExecutorInfo.remove(slaveID.getValue)
    }
  }

  //重新注册，貌似不使用
  override def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo): Unit = {
    logger.info(s"Framework re-registered with master ${masterInfo.getId}")
  }

  //框架消息，貌似不可用
  override def frameworkMessage(schedulerDriver: SchedulerDriver, executorID: Protos.ExecutorID, slaveID: Protos.SlaveID, bytes: Array[Byte]): Unit = {
    logger.info(s"executor[$executorID] slaveID[$slaveID] message[${new String(bytes)}]")
  }


  //executor 失效，从任务角度来处理
  override def executorLost(schedulerDriver: SchedulerDriver, executorID: Protos.ExecutorID, slaveID: Protos.SlaveID, i: Int): Unit = {
    logger.info(s"Framework slave executor lost on $slaveID")
    stateLock.synchronized {
      //删除slave到executor的映射关系
      slaveIdToExecutorInfo.remove(slaveID.getValue)
    }
  }

  //当提交任务到slave的时候,恰好slave不可用，会有这个回调
  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {
    logger.info(s"Framework offer rescinded on $offerId")
  }

  //end of scheduler


  //延迟计算
  private def schedulerDriver(): Option[SchedulerDriver] = {
    try {
      val builder = FrameworkInfo.newBuilder().setName(config.APP_NAME)
      checkpoint.foreach { checkpoint => builder.setCheckpoint(checkpoint) }
      failoverTimeout.foreach { timeout => builder.setFailoverTimeout(timeout) }
      frameworkIDStore.getFrameworkID.foreach { id =>
        builder.setId(FrameworkID.newBuilder().setValue(id).build())
      }
      builder.addCapabilities(Capability.newBuilder().setType(Capability.Type.MULTI_ROLE))
      builder.setUser(config.MESOS_USER)
      builder.addRoles(config.MESOS_ROLE)
      //      builder.setHostname(config.HOST_NAME)
      val credential: Credential = Credential.newBuilder()
        .setPrincipal(config.MESOS_PRINCIPAL)
        .setSecret(config.MESOS_SECRET)
        .build()
      Some(new MesosSchedulerDriver(AtlasScheduler.this, builder.build(), config.MESOS_MASTER_URL, credential))
    } catch {
      case ex: Throwable =>
        logger.error("fail on create scheduler driver", ex)
        throw ex
    }
  }

  /**
    * 启动scheduler　
    */
  def run(): Unit = {
    val status = schedulerDriver()
      .getOrElse(throw new RuntimeException("生成scheduler driver 失败")).run()
    logger.error(s"exit on $status")
  }

  def createExecutorInfo(
                          execId: String): ExecutorInfo = {
    val environment = Environment.newBuilder()
    val extraJavaOpts = ""

    environment.addVariables(
      Environment.Variable.newBuilder()
        .setName("ATLAS_EXECUTOR_OPTS")
        .setValue(extraJavaOpts)
        .build())
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)

    val executorBackendName = classOf[AtlasExecutor].getName
    command.setValue(s"java -classpath  /opt/mesos-bin-jar-with-dependencies.jar $executorBackendName")
    val builder = ExecutorInfo.newBuilder()

    //CPU信息
    val cpuResourcesToUse = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(1).build()).build()

    //内存
    val memResourcesToUse = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(256).build()).build()


    builder.addResources(cpuResourcesToUse)
    builder.addResources(memResourcesToUse)

    val executorInfo = builder
      .setExecutorId(ExecutorID.newBuilder().setValue(execId).build())
      .setCommand(command)

    executorInfo.setContainer(ContainerInfo.newBuilder().setType(ContainerInfo.Type.MESOS))
    executorInfo.build()
  }
}
