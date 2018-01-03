package com.atlas

import java.util.Properties

import org.slf4j.LoggerFactory

object Startup {
  private val LOG = LoggerFactory.getLogger("Startup")

  def main(args: Array[String]): Unit = {
    try {
      //使用using自动关闭打开的文件资源
      val config = AtlasUtils.closable(Startup.getClass.getClassLoader.getResourceAsStream("application.properties")) { source =>
        val properties = new Properties()
        properties.load(source)
        new SchedulerConfig(properties)
      }

      LOG.info(s"init atlas service with ${config.toString}")
      //zk客户端
      val client = CuratorUtils.client(config)
      //mesos driver
      val driver = new AtlasDriver(frameworkIDStore = new ZKFrameworkIDStore(client, config), config = config)
      //zk leader
      if (config.HA_ENABLE) {
        new LeaderSelector(driver = driver, client = client, config.ZK_LEADER_PATH).run()
      } else {
        try {
          driver.run()
        } finally {
          LOG.error("release leadership.")
        }
      }
    } catch {
      case e: Exception =>
        LOG.error("start atlas service error", e)
        System.exit(-1)
    }
  }
}

