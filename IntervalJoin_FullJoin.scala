package com.imooc.scala.sqljoin

import java.time.ZoneId

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 时间区间Join（Interval Join）之 Full Join
 * Created by xuwei
 */
object IntervalJoin_FullJoin {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //指定国内的时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    //设置Source自动置为idle的时间--如果数据源可以一直正常产生数据，则不需要配置此参数！！！
    tEnv.getConfig.set("table.exec.source.idle-timeout","10s")

    //订单表
    val UserOrderTableSql =
      """
        |CREATE TABLE user_order(
        |  order_id BIGINT,
        |  -- 事件时间戳
        |  ts BIGINT,
        |  -- 转换事件时间戳为TIMESTAMP_LTZ(3)类型
        |  d_timestamp AS TO_TIMESTAMP_LTZ(ts,3),
        |  user_id STRING,
        |  -- 通过Watermark定义事件时间，可以在这里指定允许的最大乱序时间
        |  -- 例如：WATERMARK FOR d_timestamp AS d_timestamp - INTERVAL '5' SECOND
        |  WATERMARK FOR d_timestamp AS d_timestamp
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'user_order',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-order',
        |  -- 为了便于演示，在这使用latest-offset，每次启动都使用最新的数据
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(UserOrderTableSql)

    //支付表
    val PaymentFlowTableSql =
      """
        |CREATE TABLE payment_flow(
        |  order_id BIGINT,
        |  ts BIGINT,
        |  d_timestamp AS TO_TIMESTAMP_LTZ(ts,3),
        |  pay_money BIGINT,
        |  WATERMARK FOR d_timestamp AS d_timestamp
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'payment_flow',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-payment',
        |  -- 为了便于演示，在这使用latest-offset，每次启动都使用最新的数据
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(PaymentFlowTableSql)


    //结果表
    val resTableSql =
      """
        |CREATE TABLE order_payment(
        |  order_id BIGINT,
        |  user_id STRING,
        |  pay_money BIGINT
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'order_payment',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'format' = 'json',
        |  'sink.partitioner' = 'default'
        |)
        |""".stripMargin
    tEnv.executeSql(resTableSql)

    //关联订单表和支付表
    val joinSql =
      """
        |INSERT INTO order_payment
        |SELECT
        |  case when pf.order_id is null then uo.order_id else pf.order_id end,
        |  uo.user_id,
        |  pf.pay_money
        |FROM user_order AS uo
        |-- 这里使用FULL JOIN 或者FULL OUTER JOIN 是一样的效果
        |FULL JOIN payment_flow AS pf ON uo.order_id = pf.order_id
        |-- 指定取值的时间区间（前后10分钟）
        |AND uo.d_timestamp
        |  BETWEEN pf.d_timestamp - INTERVAL '10' MINUTE
        |  AND pf.d_timestamp + INTERVAL '10' MINUTE
        |""".stripMargin
    tEnv.executeSql(joinSql)

  }

}
