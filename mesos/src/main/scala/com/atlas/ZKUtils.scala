package com.atlas

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory._
import org.apache.curator.retry.ExponentialBackoffRetry

object CuratorUtils {
  def client(config: SchedulerConfig): CuratorFramework = {
    val client = builder.connectString(config.ZK_CONNECT_ADDRESS)
      .sessionTimeoutMs(10000)
      .connectionTimeoutMs(10000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build
    client.start()
    client
  }
}

class ZKFrameworkIDStore(client: CuratorFramework, config: SchedulerConfig) extends FrameworkIDStore {
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

    print(s"""stored as frameworkID:$frameworkID""")
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

