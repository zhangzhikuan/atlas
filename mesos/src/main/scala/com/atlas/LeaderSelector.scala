package com.atlas

import java.io.{Closeable, IOException}
import java.net.InetAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelectorListenerAdapter, LeaderSelector => ZKLeaderSelector}
import org.slf4j.LoggerFactory


class LeaderSelector(scheduler: AtlasScheduler, client: CuratorFramework, path: String
                    ) extends LeaderSelectorListenerAdapter with Closeable {


  private val log = LoggerFactory.getLogger(classOf[LeaderSelector])

  private val wait_lock = new CountDownLatch(1)
  private val started = new AtomicBoolean(false)

  private val leaderSelector: Option[ZKLeaderSelector] = Some(new ZKLeaderSelector(client, path, this))
  leaderSelector.foreach(_.autoRequeue())

  Runtime.getRuntime.addShutdownHook {
    new Thread() {
      override def run(): Unit = {
        close()
        log.info("killed by signal")
        wait_lock.countDown()
      }
    }
  }

  @throws[Exception]
  def start(): Unit = {
    leaderSelector.foreach {
      selector =>
        selector.start()
        started.set(true)
    }
    log.info("leader selector is running")
    wait_lock.await()
  }


  @throws[Exception]
  protected override def takeLeadership(client: CuratorFramework): Unit = {
    log.info("now the leader is " + InetAddress.getLocalHost.getHostName)
    try {
      scheduler.run()
    } finally {
      log.error("release leadership.")
    }
  }

  @throws[IOException]
  override def close(): Unit = {
    leaderSelector.foreach {
      selector =>
        if (started.get()) {
          selector.close()
          started.set(false)
        }
    }
  }
}
