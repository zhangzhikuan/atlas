package com.atlas

import java.util.Properties

import org.slf4j.LoggerFactory

object Startup {
  private val log = LoggerFactory.getLogger("Startup")


  def main(args: Array[String]): Unit = {
    try {
      //使用using自动关闭打开的文件资源
      val config = Utils.closable(Startup.getClass.getClassLoader.getResourceAsStream("application.properties")) { source =>
        val properties = new Properties()
        properties.load(source)
        new SchedulerConfig(properties)
      }

      log.info(s"init atlas service with ${config.toString}")
      val client = CuratorUtils.client(config)
      val scheduler = new AtlasScheduler(frameworkIDStore = new ZKFrameworkIDStore(client, config), config = config)
      val leaderSelector = new LeaderSelector(scheduler, client = client, config.ZK_LEADER_PATH)
      if (config.HA_ENABLE) {
        leaderSelector.start()
      } else {
        try {
          scheduler.run()
        } finally {
          log.error("release leadership.")
        }
      }
    } catch {
      case e: Exception =>
        log.error("start atlas service error", e)
        System.exit(-1)
    }
  }


}

