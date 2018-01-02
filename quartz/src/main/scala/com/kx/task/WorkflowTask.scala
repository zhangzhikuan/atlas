package com.kx.task

case class WorkflowTask(
                         workflowId: Long,
                         taskId: Long,
                         pid: Option[Long]
                       )

case class WorkflowExecution(workflowId: Long,
                             workflowInstanceId: Long,
                             batchId: String,
                             taskId: Option[Long]
                            )

object WorkflowTask {
  val wf1 = List(
    WorkflowTask(1L, 1L, None)
    , WorkflowTask(1L, 2L, Some(1L))
    , WorkflowTask(1L, 3L, Some(1L))
    , WorkflowTask(1L, 33L, Some(1L))
    , WorkflowTask(1L, 4L, Some(2L))
    , WorkflowTask(1L, 4L, Some(3L))
    , WorkflowTask(1L, 4L, Some(33L))
    , WorkflowTask(1L, 5L, Some(4L))
    , WorkflowTask(1L, 6L, Some(5L))
    , WorkflowTask(1L, 7L, Some(5L))
  )
}
