package com.atlas

import org.apache.mesos.Protos._

import scala.collection.JavaConverters._

class TaskBuilder(offer: Offer) {
  private val builder = TaskInfo.newBuilder()
    .setSlaveId(offer.getSlaveId)
    .setContainer(ContainerInfo.newBuilder().setType(ContainerInfo.Type.MESOS))

  /**
    * 任务信息
    *
    * @param taskId   任务ID
    * @param taskName 任务名称
    * @return
    */
  def withTaskInfo(taskId: String, taskName: String): TaskBuilder = {
    val mesos_task_id = TaskID.newBuilder().setValue(taskId).build()
    builder.setTaskId(mesos_task_id).setName(taskName)
    this
  }

  /**
    * 执行的命令
    *
    * @param command 待执行的命令
    * @return
    */
  def withCommand(command: String): TaskBuilder = {
    //执行命令
    val commandInfo = CommandInfo.newBuilder().setValue(command).build()
    builder.setCommand(commandInfo)
    this
  }

  /**
    * 资源信息
    *
    * @param cpus   cpu
    * @param memory 内存
    * @return
    */
  def withResource(cpus: Double = 0.1d,
                   memory: Long = 32): TaskBuilder = {

    //CPU信息
    val cpuResourcesToUse = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(cpus).build()).build()

    //内存
    val memResourcesToUse = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(memory).build()).build()

    //val label = Protos.Label.newBuilder().setKey(cleanedParts(0)).setValue(cleanedParts(1)).build()

    //创建mesos任务
    builder
      .addAllResources(List(cpuResourcesToUse).asJava)
      .addAllResources(List(memResourcesToUse).asJava)
    this
  }

  /**
    * 创建任务
    *
    * @return
    */
  def build(): TaskInfo = {
    if (!builder.hasName) {
      throw new RuntimeException("task name not set")
    }
    if (!builder.hasTaskId) {
      throw new RuntimeException("task id not set")
    }
    if (!builder.hasCommand) {
      throw new RuntimeException("task command not set")
    }
    if (builder.getResourcesBuilderList.size() != 2) {
      throw new RuntimeException("resource not set")
    }
//    ExecutorInfo.newBuilder().setCommand(CommandInfo.newBuilder().addUris(URI.newBuilder().setValue("")).setValue(""))
    builder.build()
  }
}

