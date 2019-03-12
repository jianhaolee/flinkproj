import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

import scala.collection.mutable
import scala.collection.mutable.Set

case class ReduceWithRedisFunctionState() extends AggregateFunction[(String,String,String,String,String),(Long,mutable.Set[String])
  ,(String,String,String,String,String)] with Serializable with CheckpointedFunction{
  /**
    * 这里的对象  使用时要进行传递，  例如
    * (acc._1.+(1),acc._2)
    * 如果，只在方法内对对象进行调用，  会出问题  ！！！！  数据新增异常，或set跨窗口现象
    */
  var pv:Long=0

  var UVACC:mutable.Set[String]=_
  var projectcode:String=""
  var hour:String=""

  @transient
//  private var checkpointedState: ValueState[Set[String]] = _
  private var checkpointedState: ListState[(String,Set[String])] = _


//  var jedisCluster:Je
  override def createAccumulator(): (Long,mutable.Set[String]) = {
    UVACC=Set()
    pv=0
    (pv,UVACC)
  }
  override def add(in: (String, String, String, String,String), acc: (Long,mutable.Set[String])): (Long,mutable.Set[String]) = {
//    格式   日期，时间，id，项目码
    val projectcode = in._4
    val date = in._1
    val time = in._2
    val openid=in._5
    val hour = time.split(":")(0)

    this.projectcode=projectcode
    this.hour=hour


    //记录pv
    UVACC.add(openid)
    pv+=1
    println(Thread.currentThread().getName+"     " +projectcode+"   "+hour+"   "+pv+"--"+UVACC.size+"  ")
    (pv,UVACC)
//    acc._2.add(openid)
//    println(Thread.currentThread().getName+"     " +hour+"   "+(acc._1.+(1))+"--"+UVACC.size+"  ")
//    (acc._1.+(1),acc._2)
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

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    val list = new util.ArrayList[((String,mutable.Set[String]))]()
    list.add(("UVS-"+projectcode+"-"+hour,UVACC))
    checkpointedState.update(list)
  }

  override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit ={
    val descriptor = new ListStateDescriptor[(String,Set[String])](
      "projectcode-hour-uvs",
      TypeInformation.of(new TypeHint[(String,Set[String])]() {})
    )

    checkpointedState = functionInitializationContext.getOperatorStateStore.getListState(descriptor)

    if(functionInitializationContext.isRestored) {

      for(element:(String,Set[String])<-checkpointedState.get()){
        if (element._1.startsWith("UVS-")){
          this.UVACC=element._2
        }
      }

    }
  }
}
