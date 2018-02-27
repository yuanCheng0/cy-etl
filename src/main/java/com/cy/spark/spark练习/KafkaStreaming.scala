package com.cy.spark.spark练习

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaStreaming {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if(args.length > 0 ){
      masterUrl = args(0)
    }
//    val properties: util.List[String] = JsonU.readProperties(file)
    //构建spark streaming 上下文
    val conf = new SparkConf().setMaster(masterUrl).setAppName("spark streaming application")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf,Seconds(5))
    val hiveContext = new HiveContext(sc)
    ssc.checkpoint("D:\\checkpoint")
    val topics = Set("web_log")
    val brokers = "10.10.4.126:9092,10.10.4.127:9092"
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val orginLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
    //处理数据
    parseLog(sc,orginLogDStream)
    //启动、等待执行结束、关闭
    ssc.start()
    ssc.awaitTermination()
  }
  //处理数据
  def parseLog(sc: SparkContext,orginLogDStream: InputDStream[(String, String)]): Unit = {
    //最后需要实时维护的的是每天接收数据的总条数，pageEvent数据总条数,pageData数据总条数,错误数据总条数
    val errorLines:List[String] = null
//    val errorLinesRdd = sc.parallelize(errorLines)
    //用于统计每天接收数据总量
    val allDataDStream = orginLogDStream.map(data => {
      val now:Date = new Date()
      val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date = dateFormat.format( now )
      //获取每行日志原始数据
      val line = data._2
      var pageEventBean: PageEventBean = null
      var pageDataBean: PageDataBean = null
      var errorLine: String = null
      if (StringUtils.isBlank(line)) {
        //无效数据 ---> 添加到错误数据中
        errorLine = line
//        errorLines.+:(line)
      } else {
        //清除无效字符 。。。。TODO
        val arrLines = line.split("|")
        if (arrLines.length >= 16) {
          //TODO 假设全解析并封装完成
          val nginxLogBean: NginxLogBean = null
          //一条数据要么属于pageEventBean，要么属于pageDataBean
          try{
            if (true){ // TODO 此处只是假设 start 待改
              pageEventBean = new PageEventBean
            }else{
              pageDataBean = new PageDataBean
            }
            // TODO 假设end
          }catch{
            case e:_ => {
              errorLine = line
//              errorLines.+:(line)
            }
          }finally {
          }
        } else {
          errorLine = line
//          errorLines.+:(line)
        }
      }
      (date,errorLine,pageEventBean,pageDataBean)
    })
    val totalCountDStream = allDataDStream.map(data => data._1)
    val pageEventCountDStream = allDataDStream.transform(rdd => {
      val pageEventRdd = rdd.filter(data => data._3 != null).map(data => (data._3))

      rdd.map(data => ("pageEvent",1L))
    })
   /* val pageEventDStream = orginLogDStream.map(data => {

    })*/

    /*val aggregatedDStream = orginLogDStream.map(data => {
      //获取每行日志原始数据
      val line = data._2
      if (StringUtils.isBlank(line)) {
        //无效数据 ---> 添加到错误数据中
        errorLines.+:(line)
      } else {
        //清除无效字符 。。。。TODO
        val arrLines = line.split("|")
        if (arrLines.length >= 16) {
        } else {
          errorLines.+:(line)
        }
      }
      //返回总条数
      ((line,1L),(line,1L))
    })*/
//    aggregatedDStream.updateStateByKey[Int]()
    /*orginLogDStream.transform(rdd => {
      rdd.map(data => {
        //获取每行日志原始数据
        val line = data._2
        if (StringUtils.isBlank(line)){
          //无效数据 ---> 添加到错误数据中
          errorLines.+:(line)
        }else{
          //清除无效字符 。。。。TODO
          val arrLines = line.split("|")
          if (arrLines.length >= 16){
          }else{
            errorLines.+:(line)
          }
        }
      })
    })*/
  }
}
