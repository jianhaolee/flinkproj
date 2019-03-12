import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommandDescription, RedisMapper}
import redis.clients.jedis.JedisCluster
import sinks.MyRedisRichMapper

class MyRedisMapper extends MyRedisRichMapper[(String,(String,String))] with Serializable {

  private val serialVersionUID=456L

  override def getCommandDescription: RedisCommandDescription ={
    null
  }


  override def getKeyFromData(data: (String, (String, String))): String = ???

  override def getValueFromData(data: (String, (String, String))): String = ???

  /**
    * 自定义的通用方法
    * @param input
    * @param jedisCluster
    */
  override def handlFunction(input: (String, (String, String)), jedisCluster: JedisCluster): Unit = {
    val key = input._1
    val pv = input._2._1
    val uv = input._2._2

    jedisCluster.hset(key,"pv",pv)
    jedisCluster.hset(key,"uv",uv)
  }
}
