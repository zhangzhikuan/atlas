package com.atlas

import org.apache.mesos.Protos._

import scala.collection.mutable

trait MesosUtils {

  protected def resourceToUse(scalar: Double, typ: String): Resource = {
    Resource.newBuilder()
      .setName(typ)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(scalar).build()).build()
  }

  //slave 和 executor 的关联关系
  private val slaveIdToExecutorInfo = new mutable.HashMap[String, ExecutorInfo]()

  //获取或创建
  protected def getOrCreateExecutor(slaveIDInfo: SlaveID): ExecutorInfo = {
    val slaveID: String = slaveIDInfo.getValue
    slaveIdToExecutorInfo.getOrElseUpdate(slaveID, {
      slaveIdToExecutorInfo.put(slaveID, createExecutorInfo(s"atlas_executor_$slaveID"))
      slaveIdToExecutorInfo(slaveID)
    })
  }

  //删除executor
  protected def removeExecutor(slaveIDInfo: SlaveID): Unit = {
    //删除slave到executor的映射关系
    slaveIdToExecutorInfo.remove(slaveIDInfo.getValue)
  }

  //创建executor
  private def createExecutorInfo(execId: String): ExecutorInfo = {
    val environment = Environment.newBuilder()
    //环境变量java参数
    val extraJavaOpts = Environment.Variable.newBuilder()
      .setName("ATLAS_EXECUTOR_OPTS").setValue("").build()
    environment.addVariables(extraJavaOpts)

    //executor启动命令
    val command = CommandInfo.newBuilder().setEnvironment(environment)

    //executor启动时的进程名称，可以通过jps查询
    val executorBackendName = classOf[AtlasExecutor].getName
    command.setValue(s"java -classpath  /opt/mesos-bin-jar-with-dependencies.jar $executorBackendName")

    val memResourcesToUse: Resource = resourceToUse(256, "mem")
    val cpusResourcesToUse: Resource = resourceToUse(1, "cpus")

    //Executor的配置项
    ExecutorInfo.newBuilder()
      .addResources(cpusResourcesToUse)
      .addResources(memResourcesToUse)
      .setExecutorId(ExecutorID.newBuilder().setValue(execId).build())
      .setCommand(command).setContainer(ContainerInfo.newBuilder().setType(ContainerInfo.Type.MESOS))
      .build()
  }


}
