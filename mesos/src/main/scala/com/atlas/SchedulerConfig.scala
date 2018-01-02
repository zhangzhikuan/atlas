package com.atlas

import java.util.Properties

class SchedulerConfig(properties: Properties) {

  //APP名称
  val APP_NAME = s"${properties.getProperty("atlas.app.name")}"
  //zk地址
  val ZK_CONNECT_ADDRESS: String = properties.getProperty("atlas.zk.address")
  //mesos master 地址
  val MESOS_MASTER_URL = s"zk://$ZK_CONNECT_ADDRESS/mesos"
  //执行命令的用户名
  val MESOS_USER: String = properties.getProperty("atlas.mesos.user", System.getProperty("user.name"))
  val MESOS_ROLE: String = properties.getProperty("atlas.mesos.role", "*")
  val MESOS_SECRET: String = properties.getProperty("atlas.mesos.secret")
  val MESOS_PRINCIPAL: String = properties.getProperty("atlas.mesos.principal")
  //Framework的id存放路径
  val ZK_FRAMEWORK_ID_PATH = s"${properties.getProperty("atlas.zk.path.frameworkid")}/$APP_NAME"

  //是否开启HA
  val HA_ENABLE: Boolean = properties.getProperty("atlas.scheduler.ha.enable").toBoolean
  //选举LEADER时使用的路径
  val ZK_LEADER_PATH = s"${properties.getProperty("atlas.zk.path.leader")}/$APP_NAME"
  val HOST_NAME: String =properties.getProperty("atlas.scheduler.hostname",java.net.InetAddress.getLocalHost.getHostName)
  override def toString = s"SchedulerConfig(APP_NAME=$APP_NAME, ZK_CONNECT_ADDRESS=$ZK_CONNECT_ADDRESS, MESOS_MASTER_URL=$MESOS_MASTER_URL, MESOS_USER=$MESOS_USER, MESOS_ROLE=$MESOS_ROLE, MESOS_SECRET=$MESOS_SECRET, MESOS_PRINCIPAL=$MESOS_PRINCIPAL, ZK_FRAMEWORK_ID_PATH=$ZK_FRAMEWORK_ID_PATH, HA_ENABLE=$HA_ENABLE, ZK_LEADER_PATH=$ZK_LEADER_PATH)"
}
