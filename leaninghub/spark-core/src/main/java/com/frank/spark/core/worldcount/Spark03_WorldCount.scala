package com.frank.spark.core.worldcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WorldCount {


  def main(args: Array[String]): Unit = {
    //  application
    // spark 框架

    // 建立和 spark 框架的连接
    // jdbc: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sc = new SparkContext(sparkConf)

    // 执行 业务操作
    // 1. 读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile("datas")
    // 2. 将一行数据进行拆分，形成一个个的单词（分词）
    // "hello world" => hello, world, hello, world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      words => (words, 1)
    )

    // spark 框架提供了更多的功能，可以将分组和聚合使用一个 方法实现
    // reduceByKey: 相同的key的数据，可以对value 进行reduce 聚合
//    wordToOne.reduceByKey((x, y) => {x+y});
//    wordToOne.reduceByKey((x, y) => x+y);
    val wordToCount = wordToOne.reduceByKey(_+_);

    // 5. 将装换结果采集到控制台进行打印
    val array: Array[(String, Int)] = wordToCount.collect();
    array.foreach(println)


    // 关闭连接
    sc.stop()
  }

}
