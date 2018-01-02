package com.atlas

trait FrameworkIDStore {
  def storeFrameworkID(frameworkID: String): Unit = {
    print(s"""stored as frameworkID:$frameworkID""")
  }
  def getFrameworkID: Option[String] = None
}


