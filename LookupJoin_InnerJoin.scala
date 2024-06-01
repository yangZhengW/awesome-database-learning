package com.imooc.scala.sqljoin

import java.time.ZoneId

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * 维表Join（Lookup Join）之 Inner Join
 * Created by xuwei
 */
object LookupJoin_InnerJoin {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tEnv = TableEnvironment.create(settings)

    //设置全局并行度为1
    tEnv.getConfig.set(CoreOptions.DEFAULT_PARALLELISM.key(),"1")

    //指定国内的时区
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))


    //直播开播记录表
    val VideoDataTableSql =
      """
        |CREATE TABLE video_data(
        |  vid STRING,
        |  uid STRING,
        |  start_time BIGINT,
        |  country STRING,
        |  proc_time AS PROCTIME() -- 处理时间
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'video_data',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'properties.group.id' = 'gid-sql-video',
        |  'scan.startup.mode' = 'latest-offset',
        |  'format' = 'json',
        |  'json.fail-on-missing-field' = 'false',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |""".stripMargin
    tEnv.executeSql(VideoDataTableSql)

    //国家和大区映射关系-维表
    val CountryAreaTableSql =
      """
        |CREATE TABLE country_area(
        |  country STRING,
        |  area STRING
        |)WITH(
        |  'connector' = 'jdbc',
        |  'driver' = 'com.mysql.cj.jdbc.Driver', -- mysql8.x使用这个driver class
        |  'url' = 'jdbc:mysql://localhost:3306/flink_data?serverTimezone=Asia/Shanghai', -- mysql8.x中需要指定时区
        |  'username' = 'root',
        |  'password' = 'admin',
        |  'table-name' = 'country_area',
        |  -- 通过lookup缓存可以减少Flink任务和数据库的请求次数，启用之后每个子任务中会保存一份缓存数据
        |  'lookup.cache.max-rows' = '100', -- 控制lookup缓存中最多存储的数据条数
        |  'lookup.cache.ttl' = '3600000', -- 控制lookup缓存中数据的生命周期(毫秒)，太大或者太小都不合适
        |  'lookup.max-retries' = '1' -- 查询数据库失败后重试的次数
        |)
        |""".stripMargin
    tEnv.executeSql(CountryAreaTableSql)


    //结果表
    val resTableSql =
      """
        |CREATE TABLE new_video_data(
        |  vid STRING,
        |  uid STRING,
        |  start_time BIGINT,
        |  area STRING
        |)WITH(
        |  'connector' = 'kafka',
        |  'topic' = 'new_video_data',
        |  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',
        |  'format' = 'json',
        |  'sink.partitioner' = 'default'
        |)
        |""".stripMargin
    tEnv.executeSql(resTableSql)

    //关联开播记录表和国家大区关系表
    val joinSql =
      """
        |INSERT INTO new_video_data
        |SELECT
        |  vid,
        |  uid,
        |  start_time,
        |  area
        |FROM video_data
        |INNER JOIN country_area FOR SYSTEM_TIME AS OF video_data.proc_time
        |ON video_data.country = country_area.country
        |""".stripMargin
    tEnv.executeSql(joinSql)

  }

}
