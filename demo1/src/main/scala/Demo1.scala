import java.net.InetSocketAddress
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import sinks.{MyRedisSinkScala, MyRedisSinkScala3}



object Demo1 extends Serializable {


  def main(args: Array[String]): Unit = {

    println("start")
    import org.apache.flink.api.scala._

    //1.准备环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)//设置并行度
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//设置日志时间

    //2.连接主kafka
    val mainConsumer = initMainKafka()
    val standbyConsumer = initStandbyKafka()
//    mainConsumer.setStartFromEarliest();     // start from the earliest record possible
    mainConsumer.setStartFromLatest();       // start from the latest record
//    mainConsumer.setStartFromTimestamp(); // start from specified epoch timestamp (milliseconds)
//    mainConsumer.setStartFromGroupOffsets(); // the default behaviour
    //适配消费者
    val mainLog = env.addSource(mainConsumer)
    val standbyLog = env.addSource(standbyConsumer)




    //3.主备数据合并
    val data = mainLog.union(standbyLog)
//      .setParallelism(1)


    //4.只取访问数据
    val visitData = data.filter(logJson=>{
      val jSONObject = JSON.parseObject(logJson)

      val filename = jSONObject.getString(ParmsPool.getLOG_FORMAT_FILENAME())
      if (filename!=ParmsPool.getLOG_DIMENSION_VISIT()){
        //不是访问维度，过滤掉
        //        println(Thread.currentThread().getName)
//        println(logJson)
//        Thread.sleep(1000)
        true
      }else{
        //访问维度，不过滤
        false
      }
    }).uid("getVisit")

    //5.获取日志内容
    val logStream = visitData.map(json=>{
      val jsonObj = JSON.parseObject(json)
      val log = jsonObj.getString(ParmsPool.getLOG_FORMAT_CONTENT())
      log
    })

    //6.过滤安全的请求
    val saveVisitData = logStream.filter(log=>{
      var isSave =false//默认安全,即不过滤
      val args = log.split("\t")
      try{
        if(args.length>37){
          val riskLevel:String = args(37)
          isSave = (riskLevel.trim.toInt <= 1)
        }
      }catch{
        case e:NumberFormatException=>{
          try{
            if(args.length>37){
              val riskLevel:String = args(36)
              isSave = (riskLevel.trim.toInt <= 1)
            }
          }catch{
            case e :Exception=>{
              e.getCause
            }

          }
        }
      }

//      println(log)
      isSave
    }).uid("getSaveData")


    //7.获取有用数据--格式   日期，时间，id，项目码
    val parsedData = saveVisitData.map(parseLogData(_)).uid("parseData")

//    parsedData.map(println(_))

    //8.设置解析事件时间和watermarker
    val markedData = parsedData.assignTimestampsAndWatermarks(new LMLTimestampAndWatermarks()).uid("markTime")

    //9.分组 按照小时分window  统计
    val resultStream = markedData
      .keyBy(3)
//      .window(TumblingEventTimeWindows.of(Time.hours(1)))
//      .window(TumblingEventTimeWindows.of(Time.minutes(2)))
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .aggregate(new ReduceWithRedisFunction2())
      .uid("windowedAndReduced")

    bindRedisSink(resultStream)



    env.execute("lml-flink-test")


  }

  def parseLogData(log:String): Tuple5[String,String,String,String,String] ={

    val args = log.split("\t")

    val id = args(0)
    val projectcode = args(6)
    val logDate = args(19)
    val logTime = args(20)
    val openid = args(1)

//    println((logDate,logTime,id,projectcode,openid))
    //格式   日期，时间，id，项目码
    new Tuple5(logDate,logTime,id,projectcode,openid)
//    new Tuple5(logDate,logTime,id,"32010111111",openid)
//    new Tuple5("2019-02-26","15:25:26","asdasdas","32010422","openid")
  }

  def initMainKafka(): FlinkKafkaConsumer011[String] = {

//    val topic:String = "flinktest"
    val topic:String = "logger-business"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","221.122.77.3:9092,221.122.77.4:9092,221.122.77.6:9092,221.122.77.36:9092,221.122.77.37:9092")
    prop.setProperty("group.id","flinklmlrtstest")

    val mainConsumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)

    mainConsumer
  }

  def initStandbyKafka():FlinkKafkaConsumer011[String]={

//    val topic:String = "flinktest"
    val topic:String = "logger-business"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","39.96.1.30:9097,39.96.1.30:9098,39.96.1.30:9099")
    prop.setProperty("group.id","flinklmlrtstest")

    val standbyConsumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)

    standbyConsumer
  }



  def bindRedisSink(stream: DataStream[(String,(String,String))]):Unit={
    //1.单节点模式
//    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()
    //2.集群模式
//    val conf = new FlinkJedisPoolConfig.Builder().setNodes
//    stream.addSink(new RedisSink[(String, String)](conf, new RedisMapper[(String,String)]()))
    val password  = "vc7fiugig%5u85!"
    val hosts="221.122.77.6:6000,221.122.77.6:6001,221.122.77.6:6002,221.122.77.6:6003,221.122.77.6:6004,221.122.77.6:6005,221.122.77.6:6006,221.122.77.6:6009,221.122.77.6:6010"
    stream.addSink(new MyRedisSinkScala3[(String,(String,String))](hosts,new MyRedisMapper())).setParallelism(3)
//    stream.addSink(new MyRedisSinkScala2[(String,(String,String))](hosts,new MyRedisMapper())).setParallelism(3)
//    stream.addSink(new MyRedisSinkScala2[(String,(String,String))]()).setParallelism(1)
  }
}
