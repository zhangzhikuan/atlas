package com.atlas

import com.atlas.utils.Shell.ShellCommandExecutor
import com.google.protobuf.ByteString
import org.apache.mesos.Protos.TaskStatus.Reason
import org.apache.mesos.Protos.{Status, TaskStatus}
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver, Protos}
import org.slf4j.LoggerFactory

class AtlasExecutor extends Executor {
  private val LOG = LoggerFactory.getLogger(classOf[AtlasExecutor])

  def disconnected(driver: ExecutorDriver): Unit = {
    LOG.info("Executor has disconnected from the Mesos slave")
  }

  def error(driver: ExecutorDriver, message: String): Unit = {
    LOG.error("A fatal error has occurred: {}", message)
  }

  def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {
    LOG.info("Received a framework message")
  }

  def killTask(driver: ExecutorDriver, taskId: Protos.TaskID): Unit = {
    LOG.info("Killing task {}", taskId.getValue)
  }

  def launchTask(driver: ExecutorDriver, task: Protos.TaskInfo): Unit = {
    new Thread() {
      override def run(): Unit = {
        try {
          val status = TaskStatus
            .newBuilder.setTaskId(task.getTaskId)
            .setState(Protos.TaskState.TASK_RUNNING).build
          driver.sendStatusUpdate(status) //更新运行状态

          LOG.info(s"executed taskId ${task.getTaskId.getValue} command ${task.getData.toStringUtf8}")
          val command = task.getData.toStringUtf8
          val shell = new ShellCommandExecutor(command.split("\\s+"), null, null, 0L)
          shell.execute()
          if (shell.getExitCode == 0) {
            LOG.info(s"executed taskId ${task.getTaskId.getValue} output ${shell.getOutput}")

            val status = TaskStatus
              .newBuilder.setTaskId(task.getTaskId)
              .setState(Protos.TaskState.TASK_FINISHED)
              .setMessage("msg-任务执行成功")
              .setData(ByteString.copyFromUtf8("data-任务执行成功")).build
            driver.sendStatusUpdate(status) //更新成功状态
          } else {
            throw new RuntimeException(s"任务失败，返回状态为${shell.getExitCode},错误日志${shell.getOutput}")
          }
        } catch {
          case e: Exception =>
            LOG.error("Error execute task", e)
            val status = TaskStatus
              .newBuilder.setTaskId(task.getTaskId)
              .setState(Protos.TaskState.TASK_FAILED)
              .setMessage(s"msg-${e.getMessage}")
              .setReason(Reason.REASON_COMMAND_EXECUTOR_FAILED)
              .setData(ByteString.copyFromUtf8(s"data-${e.getMessage}")).build
            driver.sendStatusUpdate(status) //更新失败状态
        }
      }
    }.start()
  }

  def registered(driver: ExecutorDriver, executorInfo: Protos.ExecutorInfo, frameworkInfo: Protos.FrameworkInfo, slaveInfo: Protos.SlaveInfo): Unit = {
    LOG.info("Registered executor {} with {} through framework {}", executorInfo.getName, slaveInfo.getHostname, frameworkInfo.getName)
  }

  def reregistered(driver: ExecutorDriver, slaveInfo: Protos.SlaveInfo): Unit = {
    LOG.info("Re-registered executor with {}", slaveInfo.getHostname)
  }

  def shutdown(driver: ExecutorDriver): Unit = {
    LOG.info("Shutting down")
  }
}

object AtlasExecutor {
  def main(args: Array[String]): Unit = {
    val driver = new MesosExecutorDriver(new AtlasExecutor())
    System.exit(if (driver.run eq Status.DRIVER_STOPPED) 0 else 1)
  }
}
