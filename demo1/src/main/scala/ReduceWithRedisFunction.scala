import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import redis.MyJedisCluster
import redis.clients.jedis.JedisCluster

import scala.collection.mutable
import scala.collection.mutable.Set

case class ReduceWithRedisFunction() extends AggregateFunction[(String,String,String,String,String),(Long,mutable.Set[String])
  ,(String,String,String,String,String)] with Serializable{
  /**
    * 这里的对象  使用时要进行传递，  例如
    * (acc._1.+(1),acc._2)
    * 如果，只在方法内对对象进行调用，  会出问题  ！！！！  数据新增异常，或set跨窗口现象
    */
//  var pv:Long=0
//  var uv:Long=0

//  val UVACC:mutable.Set[String]L=Set()



  override def createAccumulator(): (Long,mutable.Set[String]) = {
    (0,Set())
  }
  override def add(in: (String, String, String, String,String), acc: (Long,mutable.Set[String])): (Long,mutable.Set[String]) = {
//    格式   日期，时间，id，项目码
    val projectcode = in._4
    val date = in._1
    val time = in._2
    val openid=in._5

    val hour = time.split(":")(0)
    //记录pv
    acc._2.add(openid)

    println(Thread.currentThread().getName+"     " +hour+"   "+acc._1+"--"+acc._2.size)
    flushToRedis("lml_FLink_"+projectcode+"_"+hour,acc._1.toString,acc._2.size.toString)
    (acc._1.+(1),acc._2)
  }
  def flushToRedis(key:String,pv:String,uv:String):Unit={
    MyJedisCluster.jedisCluster.hset(key,"pv",pv)
    MyJedisCluster.jedisCluster.hset(key,"uv",uv)
  }

  /**
    * 窗口结束时调用
    * @param acc
    * @return
    */
  override def getResult(acc: (Long,mutable.Set[String])): (String, String, String, String,String) = {
    println("getResult:"+acc)
    ("1","1","1","1","1")
  }

  override def merge(acc: (Long,mutable.Set[String]), acc1: (Long,mutable.Set[String])): (Long,mutable.Set[String]) = {

    val rs1 = acc._1+acc1._1
    val rs2 = acc._2++acc1._2
    (rs1,rs2)
  }

}
