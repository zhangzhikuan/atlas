package com.atlas

import java.util

import com.google.protobuf.ByteString
import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * 自定义atlas调度器
  */
class AtlasScheduler(frameworkIDStore: FrameworkIDStore) extends Scheduler with MesosUtils {
  //日志
  private val LOG = LoggerFactory.getLogger(classOf[AtlasScheduler])

  //全局锁
  private val stateLock = new Object()

  //注册成功监听
  override def registered(driver: SchedulerDriver, frameworkID: Protos.FrameworkID, masterInfo: Protos.MasterInfo): Unit = {
    LOG.info("Registered as framework ID " + frameworkID.getValue)
    frameworkIDStore.storeFrameworkID(frameworkID.getValue)
  }

  //任务状态回调
  override def statusUpdate(schedulerDriver: SchedulerDriver, status: Protos.TaskStatus): Unit = {
    stateLock.synchronized {
      val taskId = status.getTaskId.getValue
      status.getState match {
        case TaskState.TASK_FINISHED =>
          LOG.info(s"Received status update: taskId=$taskId state=${status.getState} message=${status.getMessage} data=${status.getData.toStringUtf8}")
        case TaskState.TASK_RUNNING =>
          LOG.info(s"Received status update: taskId=$taskId state=${status.getState}")
        case _ =>
          LOG.warn(s"Received status update: taskId=$taskId state=${status.getState} message=${status.getMessage} reason=${status.getReason}")
      }
    }
  }

  //错误回调
  override def error(schedulerDriver: SchedulerDriver, error: String): Unit = {
    LOG.info("Error received: " + error)
  }

  //核心代码
  override def resourceOffers(schedulerDriver: SchedulerDriver,
                              offers: util.List[Protos.Offer]): Unit = {
    stateLock.synchronized {
      //生成任务
      val tasks = offers.asScala.flatMap {
        offer =>
          //executor
          val executorInfo = getOrCreateExecutor(offer.getSlaveId)
          //mem
          val memResourcesToUse: Resource = resourceToUse(256, "mem")
          //cpu
          val cpusResourcesToUse: Resource = resourceToUse(1, "cpus")
          //任务名称
          val taskName = s"taskName_${new DateTime().getMillis}"
          //任务Id
          val taskId = s"taskId_${new DateTime().getMillis}"
          //执行命令
          val cmd = "ping www.baidu.com"
          //任务信息
          val taskInfo = TaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(taskId))
            .setSlaveId(offer.getSlaveId)
            .setExecutor(executorInfo)
            .setName(taskName)
            .addResources(cpusResourcesToUse)
            .addResources(memResourcesToUse)
            .setData(ByteString.copyFromUtf8(cmd))
            .build()
          //返回结果
          Iterator.single((offer, taskInfo))
      }
      //提交任务
      tasks.foreach {
        case (offer, task) =>
          schedulerDriver
            .launchTasks(util.Collections.singleton(offer.getId), util.Collections.singleton(task))
      }
      //把没用的资源还给mesos
      //offers.asScala.foreach(o => schedulerDriver.declineOffer(o.getId))
    }
  }

  //断开链接，等待重试
  override def disconnected(driver: SchedulerDriver): Unit = {
    LOG.info(s"Framework driver disconnected")
  }

  //slave失效
  override def slaveLost(schedulerDriver: SchedulerDriver, slaveID: Protos.SlaveID): Unit = {
    LOG.info(s"Framework slave lost on $slaveID")
    stateLock.synchronized {
      removeExecutor(slaveID)
    }
  }

  //重新注册，貌似不使用
  override def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo): Unit = {
    LOG.info(s"Framework re-registered with master ${masterInfo.getId}")
  }

  //框架消息，貌似不可用
  override def frameworkMessage(schedulerDriver: SchedulerDriver, executorID: Protos.ExecutorID, slaveID: Protos.SlaveID, bytes: Array[Byte]): Unit = {
    LOG.info(s"executor[$executorID] slaveID[$slaveID] message[${new String(bytes)}]")
  }


  //executor 失效，从任务角度来处理
  override def executorLost(schedulerDriver: SchedulerDriver, executorID: Protos.ExecutorID, slaveID: Protos.SlaveID, i: Int): Unit = {
    LOG.info(s"Framework slave executor lost on $slaveID")
    stateLock.synchronized {
      //删除slave到executor的映射关系
      removeExecutor(slaveID)
    }
  }

  //当提交任务到slave的时候,恰好slave不可用，会有这个回调
  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {
    LOG.info(s"Framework offer rescinded on $offerId")
  }
}
