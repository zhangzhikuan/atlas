package com.kx

import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import com.kx.Implicits._

object QuartzScheduler {
  def main(args: Array[String]): Unit = {
    val scheduler = try {
      //通过schedulerFactory获取一个调度器
      new StdSchedulerFactory().getScheduler
    } catch {
      case ex: Throwable => throw ex
    }
    try {
      scheduler.scheduleCronJob("0/5 * * * * ? ", 1L)
      //启动调度
      scheduler.start()
      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run(): Unit = {
          try
              if (!scheduler.isShutdown) {
                scheduler.shutdown()
              }
          catch {
            case e: SchedulerException => throw new RuntimeException(e)
          }
        }
      })
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }
}
