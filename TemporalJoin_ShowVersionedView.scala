package com.imooc.scala.sqljoin

import java.time.ZoneId

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 版本表视图的使用
 * Created by xuwei
 */
object TemporalJoin_ShowVersionedView {

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


    //汇率表-仅追加表
    val CurrencyRatesTableSql =
      """
        |CREATE TABLE currency_rates(
        |  currency STRING,
        |  rate DECIMAL(32,2),
        |  ts BIGINT,
        |  update_time AS TO_TIMESTAMP_LTZ(ts,3),
        |  WATERMARK FOR update_time AS update_time
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'currency_rates',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-rates',
        |  -- 为了便于演示，在这使用latest-offset，每次启动都使用最新的数据
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(CurrencyRatesTableSql)

    //定义版本表视图(间接实现版本表的效果)
    val VersionedRatesViewSql =
      """
        |CREATE VIEW versioned_rates AS
        |SELECT currency, rate, update_time -- 1:指定update_time为时间字段
        |  FROM (
        |      SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY currency -- 2:指定currency为主键
        |        ORDER BY update_time DESC      -- 3:ORDER BY中必须指定TIMESTAMP格式的时间字段
        |      ) AS row_num
        |      FROM currency_rates
        |  )
        |WHERE row_num = 1
        |""".stripMargin
    tEnv.executeSql(VersionedRatesViewSql)

    //验证视图中的数据格式
    tEnv.executeSql("select * from versioned_rates").print()


  }

}
