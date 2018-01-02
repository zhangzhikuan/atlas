package com.kx

import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext}

/**
  * 负责启动整个流程的任务
  */
@DisallowConcurrentExecution
class StartWorkflowJob extends Job {
  override def execute(context: JobExecutionContext): Unit = {
  }
}
