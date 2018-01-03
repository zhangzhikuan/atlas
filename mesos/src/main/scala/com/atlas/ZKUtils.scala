package com.atlas

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

object CuratorUtils {
  //zk 客户端
  def client(config: SchedulerConfig): CuratorFramework = {
    val client = CuratorFrameworkFactory.builder.connectString(config.ZK_CONNECT_ADDRESS)
      .sessionTimeoutMs(10000)
      .connectionTimeoutMs(10000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build
    client.start()
    client
  }
}

//framework ID 存储
class ZKFrameworkIDStore(client: CuratorFramework, config: SchedulerConfig) extends FrameworkIDStore {
  private val LOG = LoggerFactory.getLogger(classOf[ZKFrameworkIDStore])

  override def storeFrameworkID(frameworkID: String): Unit = {
    val stat = client.checkExists()
      .creatingParentContainersIfNeeded()
      .forPath(config.ZK_FRAMEWORK_ID_PATH)
    if (stat != null)
      client.setData()
        .forPath(config.ZK_FRAMEWORK_ID_PATH, frameworkID.getBytes)
    else
      client.create()
        .creatingParentContainersIfNeeded()
        .forPath(config.ZK_FRAMEWORK_ID_PATH, frameworkID.getBytes)

    LOG.info(s"""stored as frameworkID:$frameworkID""")
  }

  override def getFrameworkID: Option[String] = {
    val stat = client.checkExists()
      .creatingParentContainersIfNeeded()
      .forPath(config.ZK_FRAMEWORK_ID_PATH)
    if (stat != null) {
      val bytes = client.getData.forPath(config.ZK_FRAMEWORK_ID_PATH)
      Some(new String(bytes))
    } else {
      None
    }
  }
}

