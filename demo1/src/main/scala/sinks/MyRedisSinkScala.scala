package sinks
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, JedisCluster}


/**
  * 有jedisCluster  没序列化问题
  * @tparam IN
  */
class MyRedisSinkScala[IN] extends RichSinkFunction[IN] with Serializable {

  private val serialVersionUID = 1L
  private val LOG = LoggerFactory.getLogger(classOf[MyRedisSinkScala[_]])
  private val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
//  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6000))
  private var myRedisRichMapper:MyRedisRichMapper[IN]=null
  //  val clients = new JedisCluster(jedisClusterNodes)
  //  new JedisCluster(jedisClusterNodes, 10000, 1000, 1, password, new GenericObjectPoolConfig)
  /**
    * 解决序列化问题，方法一：直接调用，不引用
    * 方法二：
    *   加@transient字段
    *  方法三：
    *     放到open方法中
    */
    @transient private var jedisCluster:JedisCluster=null
//  private var jedisCluster:JedisCluster=null
  override def open(parameters: Configuration): Unit = super.open(parameters)

  def this(nodes:String,redisRichMapper: MyRedisRichMapper[IN],password:String)={
    this()
    myRedisRichMapper = redisRichMapper
    //初始化连接
    val nodelist = nodes.trim.split("\\,")
    for(nodeport :String<-nodelist){
      val args = nodeport.split("\\:")
      val host=args(0)
      val port=args(1)
      jedisClusterNodes.add(new HostAndPort(host,port.toInt))
    }
    //创建连接对象
    if (password==null){
      jedisCluster=new JedisCluster(jedisClusterNodes)
    }else{
      jedisCluster=new JedisCluster(jedisClusterNodes, 10000, 1000, 1, password, new GenericObjectPoolConfig)
    }
  }

  def this(nodes:String,redisRichMapper: MyRedisRichMapper[IN])={
    this(nodes,redisRichMapper,null)
  }
  override def invoke(input: IN): Unit = {
//    val key = redisSinkMapper.getKeyFromData(input)
//    val value = redisSinkMapper.getValueFromData(input)
    myRedisRichMapper.handlFunction(input,jedisCluster)
  }
}
