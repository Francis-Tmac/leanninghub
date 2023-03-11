package com.frank.spark.core.worldcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WorldCount {


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



    // 3. 将数据根据单词进行分组，便于统计
    // (hello, hello, hello), (world, world)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t=>t._1
    )

    // 4. 对分组后的数据进行转换
    // (hello, hello, hello), (world, world)
    // (hello, 3) (world, 2)
    val wordToCount: RDD[(String, Int)] = groupRDD.map{
      case (word, list) =>{
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    // 5. 将装换结果采集到控制台进行打印
    val array: Array[(String, Int)] = wordToCount.collect();
    array.foreach(println)


    // 关闭连接
    sc.stop()
  }

}
