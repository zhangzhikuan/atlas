package com.atlas.db

import org.squeryl.KeyedEntity

object Status extends Enumeration {
  type STATUS = Value
  val INIT = Value("0")
  val SUCCESS = Value("1")
  val FAIL = Value("2")
  val RUNNING = Value("3")
}

class Task(
            override val id: Long,
            val name: String,
            val cmd: String
          ) extends KeyedEntity[Long] {
}

class Workflow(
                override val id: Long,
                val name: String
              ) extends KeyedEntity[Long] {
}

class WorkflowTaskRelation(
                            override val id: Long,
                            val workflowId: Long,
                            val taskId: Long,
                            val pid: Long
                          ) extends KeyedEntity[Long] {
}

class WorkflowInstance(
                        override val id: Long,
                        val workflowId: Long,
                        val dataJson: String
                      ) extends KeyedEntity[Long] {
}

class TaskInstance(
                    override val id: Long,
                    val taskId: Long,
                    val workflowInstanceId: Long,
                    var dataJson: String ,
                    val batchId: String ,
                    var status: String = "0",
                    var reason: String = null,
                    var remainAttempts: Int = 3
                  ) extends KeyedEntity[Long] {
}
