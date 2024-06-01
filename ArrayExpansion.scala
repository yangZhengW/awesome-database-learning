package com.imooc.scala.sqljoin

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 数组炸裂（Array Expansion）
 * Created by xuwei
 */
object ArrayExpansion {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    //设置全局默认并行度
    env.setParallelism(1)

    //创建输入表
    val inTableSql =
      """
        |CREATE TABLE user_action_log (
        |  id  BIGINT,
        |  params ARRAY<STRING>
        |)WITH(
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.id.min' = '1',
        |  'fields.id.max' = '10',
        |  'fields.params.element.length' = '3'
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)

    //查看生成的数据格式
    /**
     * 1 [938, 6f9, 179]
     * 2 [085, dc0, 26f]
     * 3 [893, 833, ca0]
     */
    //tEnv.executeSql("select * from user_action_log").print()

    //创建输出表
    val outTableSql =
    """
      |CREATE TABLE print_sink(
      |  id BIGINT,
      |  param STRING
      |)WITH(
      |  'connector' = 'print'
      |)
      |""".stripMargin
    tEnv.executeSql(outTableSql)

    //业务逻辑
    val execSql =
      """
        |INSERT INTO print_sink
        |SELECT
        |  id,
        |  tmp.param
        |FROM user_action_log
        |-- 数组炸裂语法(列转行)
        |CROSS JOIN UNNEST(params) AS tmp (param)
        |""".stripMargin
    tEnv.executeSql(execSql)

  }

}
