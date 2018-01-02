package com.kx

import org.quartz._

class SchedulerHolder(scheduler: Scheduler) {
  def scheduleCronJob(expression: String, workflowId: Long): Unit = {
    val groupId = s"""workflow_$workflowId"""
    scheduleJob[CronTrigger]({
      JobBuilder.newJob(classOf[StartWorkflowJob]).withIdentity(s"Cron_Job_$workflowId", groupId)
        .usingJobData("cron", true)
    }, {
      TriggerBuilder.newTrigger
        .withIdentity(s"trigger_Cron_Job_$workflowId", groupId)
        .withSchedule(CronScheduleBuilder.cronSchedule(expression)) //使用cornTrigger规则
        .startNow
    })
  }

  private def scheduleJob[T <: Trigger](jobDetailBuilder: JobBuilder, triggerBuilder: TriggerBuilder[T]): Unit = {
    val jobDetail = jobDetailBuilder.build()
    if (!scheduler.checkExists(jobDetail.getKey)) {
      val trigger = triggerBuilder.build()
      scheduler.scheduleJob(jobDetail, trigger)
    }
    else
      println(s"${jobDetail.getKey}已经存在")
  }
}
