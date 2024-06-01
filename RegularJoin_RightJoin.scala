package com.imooc.scala.sqljoin

import java.time.ZoneId

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 普通Join（Regular Join）之 Right Join
 * Created by xuwei
 */
object RegularJoin_RightJoin {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //指定国内的时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    //订单表
    val UserOrderTableSql =
      """
        |CREATE TABLE user_order(
        |  order_id BIGINT,
        |  ts BIGINT,
        |  d_timestamp AS TO_TIMESTAMP_LTZ(ts,3)
        |  -- 注意：d_timestamp的值可以从原始数据中取，原始数据中没有的话也可以从Kafka的元数据中取
        |  -- d_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
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
        |  pay_money BIGINT
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
        |  order_id BIGINT NOT NULL,
        |  d_timestamp TIMESTAMP_LTZ(3),
        |  pay_money BIGINT,
        |  PRIMARY KEY(order_id) NOT ENFORCED
        |)WITH(
        |  'connector' = 'upsert-kafka',
        |  'topic' = 'order_payment',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'key.format' = 'json',
        |  'value.format' = 'json'
        |)
        |""".stripMargin
    tEnv.executeSql(resTableSql)

    //关联订单表和支付表
    val joinSql =
      """
        |INSERT INTO order_payment
        |SELECT
        |  pf.order_id,
        |  uo.d_timestamp,
        |  pf.pay_money
        |FROM user_order AS uo
        |-- 这里使用RIGHT JOIN 或者RIGHT OUTER JOIN 是一样的效果
        |RIGHT JOIN payment_flow AS pf ON uo.order_id = pf.order_id
        |""".stripMargin
    tEnv.executeSql(joinSql)

  }

}
