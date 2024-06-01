package com.imooc.scala.sqljoin

import java.time.ZoneId

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 查看汇率表中的数据
 * Created by xuwei
 */
object TemporalJoin_ShowRates {

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

    //验证版本表中的数据
    tEnv.executeSql("select * from currency_rates").print()

  }

}
