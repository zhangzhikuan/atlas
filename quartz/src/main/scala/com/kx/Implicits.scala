package com.kx

import org.quartz.Scheduler

object Implicits {
  implicit def schedulerHolder(scheduler: Scheduler): SchedulerHolder = {
    new SchedulerHolder(scheduler)
  }
}
