package com.imooc.scala.sqljoin

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 表函数Join（Table Function Join）之 Inner Join
 * Created by xuwei
 */
object TableFunctionJoin_InnerJoin {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //设置全局并行度为1
    tEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM.key(),"1")

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
        |  'fields.params.element.length' = '1' -- 数组中的元素长度改为1，便于后续在自定义函数中判断
        |)
        |""".stripMargin
    tEnv.executeSql(inTableSql)


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


    //注册自定义函数
    val funSql =
      """
        |CREATE FUNCTION my_column_to_row AS 'com.imooc.scala.sqljoin.MyColumnToRowFunc'
        |""".stripMargin
    tEnv.executeSql(funSql)

    //业务逻辑
    val execSql =
      """
        |INSERT INTO print_sink
        |SELECT
        |  id,
        |  tmp.param
        |FROM user_action_log , -- 注意：下面没有显式指定INNER JOIN的时候，表名后面必须有逗号
        |-- 给表起别名为tmp，并且重命名函数返回的字段，如果有多个字段，则使用逗号分割开即可
        |LATERAL TABLE (my_column_to_row(params)) tmp(param)
        |""".stripMargin

    val execSql2 =
      """
        |INSERT INTO print_sink
        |SELECT
        |  id,
        |  tmp.param
        |FROM user_action_log
        |-- 注意：显式指定INNER JOIN的时候，表名后面不能有逗号，并且在语句最后需要指定一个ON TRUE条件，这属于语法要求，类似于WHERE 1=1的效果。
        |INNER JOIN LATERAL TABLE (my_column_to_row(params)) tmp(param)
        | ON TRUE
        |""".stripMargin
    tEnv.executeSql(execSql2)

  }

}
