import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

import scala.collection.mutable
import scala.collection.mutable.Set

case class ReduceWithRedisFunction3() extends AggregateFunction[(String,String,String,String,String),(Long,mutable.Set[String],String,String)
  ,(String,(String,String))] with Serializable with CheckpointedFunction{


  @transient
  private var checkpointedState: ListState[(String, Int)] = _

  /**
    * 这里的对象  是跨window的对象，  在window间是共享的！！！！！！！！！！
    */
//  var pv:Long=0
//  var uv:Long=0

//  val UVACC:mutable.Set[String]L=Set()

  /**
    * 中间结果格式：
    *  pv，uv的Set集合，项目码，小时段
    * @return
    */
  override def createAccumulator(): (Long,mutable.Set[String],String,String) = {
    (0,Set(),"projectcode","hour")
  }

  override def add(in: (String, String, String, String,String), acc: (Long,mutable.Set[String],String,String)): (Long,mutable.Set[String],String,String) = {
//    格式   日期，时间，id，项目码
    val projectcode = in._4
    val date = in._1
    val time = in._2
    val openid=in._5

    val hour = time.split(":")(0)

    //记录pv
    acc._2.add(openid)

//    println(Thread.currentThread().getName+"     " +hour+"   "+acc._1+"--"+acc._2.size)
//    flushToRedis("lml_FLink_"+projectcode+"_"+hour,acc._1.toString,acc._2.size.toString)
    (acc._1.+(1),acc._2,projectcode,hour)
  }


  /**
    * 窗口结束时调用
    * @param acc
    * @return
    */
  override def getResult(acc: (Long,mutable.Set[String],String,String)): (String,(String,String)) = {
//    ("1","1","1","1","1")
    println("getResult:"+acc)
    val projeccode=acc._3
    val hour=acc._4
    val key = "lml_FLink_"+projeccode+"_"+hour
    val pv = acc._1.toString
    val uv = acc._2.size.toString
    println("key："+key+" pv:"+pv+"  uv:"+uv)

    (key,(pv,uv))
  }


  override def merge(a: (Long, mutable.Set[String], String, String), b: (Long, mutable.Set[String], String, String)): (Long, mutable.Set[String], String, String) = {
    (a._1+b._1,a._2++b._2,a._3,a._4)
  }


  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
//    checkpointedState.update()
  }

  override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[(String, Int)](
      "hisroty-uv",
      TypeInformation.of(new TypeHint[(String, Int)]() {})
    )

    checkpointedState = functionInitializationContext.getOperatorStateStore.getListState(descriptor)

    if(functionInitializationContext.isRestored) {
    }
  }
}
