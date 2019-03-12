import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
//格式   日期，时间，id，项目码
class LMLTimestampAndWatermarks extends Serializable with AssignerWithPeriodicWatermarks[(String,String,String,String,String)]{

  var currentMaxTimestamp:Long = 0L;

  val maxOutOfOrderness = ParmsPool.getMaxOutOfOrderness()


  val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd|hh:mm:ss")

  /**
    * 定义生成 watermark 的逻辑
    * 默认 100ms 被调用一次
    */
  override def getCurrentWatermark: Watermark =new Watermark(currentMaxTimestamp - maxOutOfOrderness)

  /**
    * 定义如何提取 timestamp
    * @param element
    * @param l
    * @return
    */
  override def extractTimestamp(element: (String,String,String,String,String), previousElementTimestamp: Long) = {
    //格式   日期，时间，id，项目码
    val logDate = element._1.toString
    val logTime = element._2.toString
    val timestamp:Long = sdf.parse(logDate+"|"+logTime).getTime
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }

}
