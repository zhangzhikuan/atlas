package com.atlas

import org.slf4j.LoggerFactory

trait FrameworkIDStore {
  //日志
  private val LOG = LoggerFactory.getLogger(classOf[FrameworkIDStore])

  def storeFrameworkID(frameworkID: String): Unit = {
    LOG.info(s"""stored as frameworkID:$frameworkID""")
  }

  def getFrameworkID: Option[String] = None
}


