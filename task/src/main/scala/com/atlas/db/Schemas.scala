package com.atlas.db

import org.squeryl.{Schema, Table}
import AtlasPrimitiveTypeMode._

object TaskDb extends Schema {
  protected val tasks: Table[Task] = table[Task]
  protected val taskInstances: Table[TaskInstance] = table[TaskInstance]

  def queryWaitingTaskInstance(limit: Int = 10): Iterator[TaskInstance] = {
    taskInstances.where {
      t =>
        (t.status === Status.INIT.toString) and (t.remainAttempts gt 0)
    }.page(0, limit).toIterator
  }

  def queryExecutingTaskInstance(taskId: Option[Long], batchId: Option[String]): Iterator[TaskInstance] = {
    taskInstances.where {
      t =>
        (t.status === Status.RUNNING.toString) and (t.taskId === taskId.?) and (t.batchId === batchId.?)
    }.toIterator
  }

  private def single(taskInstanceId: Long): Option[TaskInstance] = {
    taskInstances.where {
      t => t.id === taskInstanceId
    }.singleOption
  }


  def success(taskInstanceId: Long): Unit = {
    val currentTaskInstance = single(taskInstanceId)
    val executingTaskInstances = queryExecutingTaskInstance(currentTaskInstance.map(t => t.taskId),
      currentTaskInstance.map(t => t.batchId))
    executingTaskInstances.foreach {
      taskInstance =>
        transaction {

          //状态置为成功，重试次数减一
          taskInstance.status = Status.SUCCESS.toString
          taskInstance.remainAttempts = taskInstance.remainAttempts - 1
          taskInstance.update

          //触发下一级任务
          WorkflowTaskDb.nextTasks(taskInstance).foreach {
            r =>
              val newInstance = new TaskInstance(id = 1, taskId = r.taskId,
                workflowInstanceId = taskInstance.workflowInstanceId,
                batchId = taskInstance.batchId,
                dataJson = taskInstance.dataJson)
              taskInstances.insert(newInstance)
          }
        }
    }
  }

  def fail(taskInstanceId: Long, reason: Option[String]): Unit = {
    //修改状态，如果尝试次数还大于1，那么暂时不设置失败
    val currentTaskInstance = single(taskInstanceId)
    currentTaskInstance.foreach {
      instance =>
        transaction {
          if (instance.remainAttempts > 1) {
            instance.reason = reason.orNull
            instance.remainAttempts = instance.remainAttempts - 1
            instance.update
          } else {
            instance.status = Status.FAIL.toString
            instance.reason = reason.orNull
            instance.remainAttempts = 0
            instance.update
            val executingTaskInstances = queryExecutingTaskInstance(currentTaskInstance.map(t => t.taskId),
              currentTaskInstance.map(t => t.batchId))
            executingTaskInstances.foreach {
              t =>
                t.status = Status.FAIL.toString
                t.reason = reason.orNull
                t.remainAttempts = 0
                t.update
            }
          }
        }
    }
  }
}

object WorkflowDb extends Schema {
  private val workflows: Table[Workflow] = table[Workflow]
  private val workflowInstances: Table[WorkflowInstance] = table[WorkflowInstance]

  def workflowInstance(id: Long): Option[WorkflowInstance] = workflowInstances.where(wf => wf.id === id).singleOption

  private def workflow(id: Long): Option[Workflow] = workflows.where(wf => wf.id === id).singleOption

  def workflow(taskInstance: TaskInstance): Option[Workflow] = workflowInstance(taskInstance.workflowInstanceId)
    .flatMap(t => workflow(t.workflowId))
}

object WorkflowTaskDb extends Schema {
  private val workflowTasks: Table[WorkflowTaskRelation] = table[WorkflowTaskRelation]

  def firstTasks(workflowId: Long): Iterator[WorkflowTaskRelation] = {
    workflowTasks.where {
      wft =>
        wft.workflowId === workflowId and wft.pid === -1L
    }.toIterator
  }

  def nextTasks(taskInstance: TaskInstance): Iterator[WorkflowTaskRelation] = {
    val workflow = WorkflowDb.workflow(taskInstance)
      .getOrElse(throw
        new RuntimeException(s"workflowInstance[${taskInstance.workflowInstanceId}]taskInstance[${taskInstance.id}]工作流不存在"))
    workflowTasks.where {
      wft =>
        wft.workflowId === workflow.id and wft.pid === Option(taskInstance.taskId).getOrElse(-1L)
    }.toIterator
  }
}
