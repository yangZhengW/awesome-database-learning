package com.imooc.scala.sqljoin

import java.time.ZoneId

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 时态/快照Join（Temporal Join）之 Inner Join
 * Created by xuwei
 */
object TemporalJoin_InnerJoin {

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

    //订单表--仅追加表
    val OrdersTableSql =
      """
        |CREATE TABLE orders(
        |  order_id BIGINT,
        |  price DECIMAL(32,2),
        |  currency STRING,
        |  ts BIGINT,
        |  order_time AS TO_TIMESTAMP_LTZ(ts,3),
        |  WATERMARK FOR order_time AS order_time
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'orders',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-orders',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(OrdersTableSql)

    //汇率表-版本表
    val CurrencyRatesTableSql =
      """
        |CREATE TABLE currency_rates(
        |  currency STRING,
        |  rate DECIMAL(32,2),
        |  ts BIGINT,
        |  update_time AS TO_TIMESTAMP_LTZ(ts,3),
        |  WATERMARK FOR update_time AS update_time,
        |  PRIMARY KEY (currency) NOT ENFORCED
        |)WITH(
        |  'connector' = 'upsert-kafka',
        |  'topic' = 'currency_rates',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-rates',
        |  'key.format' = 'json',
        |  'value.format' = 'json'
        |)
        |""".stripMargin
    tEnv.executeSql(CurrencyRatesTableSql)


    //结果表
    val resTableSql =
      """
        |CREATE TABLE orders_rates(
        |  order_id BIGINT,
        |  price DECIMAL(32,2),
        |  currency STRING,
        |  rate DECIMAL(32,2),
        |  order_time TIMESTAMP_LTZ(3)
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'orders_rates',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'format' = 'json',
        |  'sink.partitioner' = 'default'
        |)
        |""".stripMargin
    tEnv.executeSql(resTableSql)

    //关联订单表和汇率表
    val joinSql =
      """
        |INSERT INTO orders_rates
        |SELECT
        |  order_id,
        |  price,
        |  orders.currency,
        |  rate,
        |  order_time
        |FROM orders
        |INNER JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time
        |ON orders.currency = currency_rates.currency
        |""".stripMargin

    //关联订单表和汇率表--SQL中给表起别名
    val joinSqlWithAs =
      """
        |INSERT INTO orders_rates
        |SELECT
        |  os.order_id,
        |  os.price,
        |  os.currency,
        |  cr.rate,
        |  os.order_time
        |FROM orders AS os
        |INNER JOIN currency_rates FOR SYSTEM_TIME AS OF os.order_time AS cr
        |ON os.currency = cr.currency
        |""".stripMargin
    tEnv.executeSql(joinSql)

  }

}
