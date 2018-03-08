package spark

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaStream {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    //    var masterUrl = "spark://hadoop.single:7077"
    if(args.length > 0 ){
      masterUrl = args(0)
    }
    //    val properties: util.List[String] = JsonU.readProperties(file)
    //构建spark streaming 上下文
    val conf = new SparkConf().setMaster(masterUrl).setAppName("spark streaming application")
    val ssc = new StreamingContext(conf,Seconds(5))
    val hc = new HiveContext(ssc.sparkContext)
    //    ssc.checkpoint("D:\\checkpoint")
    val topics = Set("chengyuan")
    val brokers = "hadoop.single:9092"
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val orginLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
    //处理数据
    //    parseLog(hc,orginLogDStream)
    //启动、等待执行结束、关闭
    /*var test: String = null
    orginLogDStream.map(data => {

    }).print()*/
    parseLog(hc,orginLogDStream)
    ssc.start()
    ssc.awaitTermination()
  }
  case class People(name: String,age: String){}

  def parseLog(hc: HiveContext, orginLogDStream: InputDStream[(String, String)]): Unit = {
    val initDataDStream = orginLogDStream.map(data => {
      var errLine: String = null
      //      var people: People = null
      var student: Students = null
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date = dateFormat.format(now)
      val line = data._2
      if (StringUtils.isBlank(line)) {
        errLine = line
      } else {
        val arrLines = line.split("\\|")
        if (arrLines.length > 2) {
          errLine = line
        } else {
          try {
            student = new Students
            student.setName(arrLines(0))
            student.setAge(arrLines(0))
            println("test。。。。。")
          } catch {
            case e: Exception => errLine = line
          }
        }
      }
      if (errLine == null) {
        ((date, 1), (errLine, date, 0), (student, date, 1))
      } else {
        ((date, 1), (errLine, date, 1), (student, date, 0))
      }
    })
    //用于统计总行数
    val totalCntDStream = initDataDStream.map(_._1)
    //获得错误数据DStream
    val errLineDStream = initDataDStream.map(_._2).filter(_._3 != 0).map(data => (data._1,data._2))
    //获得people数据
    val peopleDStream = initDataDStream.map(_._3).filter(_._3 !=0).map(data => (data._1,data._2))

    //处理数据
    processLog(hc,totalCntDStream,errLineDStream,peopleDStream)
  }

  def processLog(hc: HiveContext,totalCntDStream: DStream[(String, Int)], errLineDStream: DStream[(String,String)], peopleDStream: DStream[(Students,String)]): Unit = {
    //统计总条数
    getCnt(totalCntDStream)
    //将错误数据写入文件
    //    writeErrLogToFile(errLineDStream)
    //将people数据写入hive
    writePeopleToHive(hc,peopleDStream)
  }
  def getCnt(totalCntDStream: DStream[(String, Int)]) = {

  }
  def writeErrLogToFile(errLineDStream: DStream[(String, String)]) = {
    errLineDStream.foreachRDD(rdd => {
      var date: String = null
      rdd.map(data => {
        date = data._2
      })
      if (date == null){
        date = "23"
      }
      rdd.saveAsTextFile("D:\\spark-test" + date + "\\")
    })
  }
  def writePeopleToHive(hc: HiveContext,peopleDStream: DStream[(Students, String)]): Unit = {
    val filedStr = "name,age"
    val schema = StructType(filedStr.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    peopleDStream.foreachRDD(rdd => {
      val rowRdd = rdd.map(p => Row(p._1.getName,p._1.getAge))
      val peopleDataFrame = hc.createDataFrame(rowRdd,schema)
      peopleDataFrame.registerTempTable("people_tmp")
      //      hc.sql("insert into table mydb.people select name,age from people_tmp")
      val results = hc.sql(" select name,age from people_tmp")
      val dt = "2018-03-01"
      val output_tmp_dir = "hdfs://hadoop.single:9000/tmp/chengyuan/students/20180302/"
      results.rdd.map(r => r.mkString("\177")).repartition(5).saveAsTextFile("hdfs://hadoop.single:9000/tmp/chengyuan/students/20180302/" + System.currentTimeMillis)
      hc.sql(s"""load data inpath '$output_tmp_dir' into table mydb.students partition (dt='$dt')""")
      println("插入成功。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。")
      /*val dt = "2018-03-05"
      results.write.mode(SaveMode.Append).partitionBy(dt).saveAsTable("students")*/
    })

  }

  /*def parseLog(hc: HiveContext,orginLogDStream: InputDStream[(String, String)]): Unit = {
    val schemaString  = "name,age"
    val fields = schemaString .split(",")
    val schema = types.StructType(fields.map(fieldname => StructField(fieldname,StringType, true)))
    orginLogDStream.foreachRDD(rdd => {
      val rowRdd = rdd.filter(data => data._2.split(" ")(1) != "18").map(data => {
        val line = data._2.split(" ")
        Row(line(0), line(1))
      })
      val logDataFrame = hc.createDataFrame(rowRdd,schema)
      logDataFrame.registerTempTable("log_tmp")
      val sql = "select name,age from log_tmp"
//      val sql = "insert into people select name,age from log_tmp"
      val dataFrame = hc.sql(sql)
      dataFrame.show()
    })
  }*/


  //处理数据
  /*
    def parseLog(sc: HiveContext,orginLogDStream: InputDStream[(String, String)]): Unit = {
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
  */
}
