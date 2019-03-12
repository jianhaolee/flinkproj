import java.util.Properties

object ParmsPool extends Serializable {

  private val prop = new Properties()

  private var loaded=false

  def getLOG_FORMAT_FILENAME(): String ={
    if(!loaded){
      prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("conf.properties"))
      loaded=true
    }
    prop.getProperty("log.format.filename")
  }

  def getLOG_DIMENSION_VISIT(): String ={
    if(!loaded){
      prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("conf.properties"))
      loaded=true
    }
    prop.getProperty("log.visit.name")
  }

  def getLOG_DIMENSION_POINT(): String ={
    if(!loaded){
      prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("conf.properties"))
      loaded=true
    }
    prop.getProperty("log.prizelog.name")
  }
  def getLOG_FORMAT_CONTENT(): String ={
    if(!loaded){
      prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("conf.properties"))
      loaded=true
    }
    prop.getProperty("log.format.content")
  }


  /**
    * 获取最大允许乱序时间---窗口触发等待时长
    * 默认 10s
    * @return
    */
  def getMaxOutOfOrderness():Long ={
    var time= 10000L
    if(!loaded){
      prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("conf.properties"))
      loaded=true
    }
    val timeStr = prop.getProperty("time.max.outoforderness")
    time=timeStr.trim.toLong
    time
  }
}
