package com.imooc.scala.sqljoin

import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * 自定义UDTF函数
 * 注意：需要通过注解指定输出数据Row中的字段名称和类型，这里的字段名称随便指定即可。
 * Created by xuwei
 */
@FunctionHint(output = new DataTypeHint("ROW<f0 STRING>"))
class MyColumnToRowFunc extends TableFunction[Row]{

  /**
   * 函数的核心业务逻辑。
   * 进来一条数据，这个函数会被触发执行一次，输出0条或者多条数据
   * @param arrs
   */
  def eval(arrs: Array[String]): Unit ={
    arrs.foreach(arr=>{
      //在这里只输出数组中元素为a-z的，便于后续判断INNER JOIN和LEFT JOIN的区别
      if(arr.matches("[a-z]")){
        collect(Row.of(arr))
      }
    })
  }

}
