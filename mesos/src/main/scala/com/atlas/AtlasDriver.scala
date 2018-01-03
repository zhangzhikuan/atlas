package com.atlas

import org.apache.mesos.Protos.FrameworkInfo.Capability
import org.apache.mesos.Protos._
import org.apache.mesos.{MesosSchedulerDriver, Scheduler, SchedulerDriver}
import org.slf4j.LoggerFactory


/**
  * atlas调度器
  *
  * @param config          配置文件
  * @param checkpoint      备份地址
  * @param failoverTimeout 重试最长时间
  */
class AtlasDriver(config: SchedulerConfig,
                  checkpoint: Option[Boolean] = None,
                  failoverTimeout: Option[Double] = Some(Int.MaxValue),
                  frameworkIDStore: FrameworkIDStore) extends MesosUtils {

  private val LOG = LoggerFactory.getLogger(classOf[AtlasDriver])

  //mesos调度
  private val scheduler = new AtlasScheduler(frameworkIDStore = frameworkIDStore)

  //创建mesos driver
  private def schedulerDriver(): Option[SchedulerDriver] = {
    try {
      val frameworkInfoBuilder = FrameworkInfo.newBuilder().setName(config.APP_NAME)
      //备份路径
      checkpoint.foreach { checkpoint => frameworkInfoBuilder.setCheckpoint(checkpoint) }
      //重试时间
      failoverTimeout.foreach { timeout => frameworkInfoBuilder.setFailoverTimeout(timeout) }
      //框架ID
      frameworkIDStore.getFrameworkID.foreach { id =>
        frameworkInfoBuilder.setId(FrameworkID.newBuilder().setValue(id).build())
      }
      //支持多role
      frameworkInfoBuilder.addCapabilities(Capability.newBuilder().setType(Capability.Type.MULTI_ROLE))
      frameworkInfoBuilder.setUser(config.MESOS_USER).addRoles(config.MESOS_ROLE)
      //权限控制
      val credential: Credential = Credential.newBuilder()
        .setPrincipal(config.MESOS_PRINCIPAL).setSecret(config.MESOS_SECRET)
        .build()
      Some(new MesosSchedulerDriver(scheduler, frameworkInfoBuilder.build(), config.MESOS_MASTER_URL, credential))
    } catch {
      case ex: Throwable =>
        LOG.error("fail create scheduler driver", ex)
        throw ex
    }
  }

  /**
    * 启动scheduler　
    */
  def run(): Unit = {
    val status = schedulerDriver().getOrElse(throw new RuntimeException("scheduler driver未生成")).run()
    LOG.error(s"exit on $status")
  }
}
