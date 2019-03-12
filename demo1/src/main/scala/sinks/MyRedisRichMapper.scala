package sinks

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper
import redis.clients.jedis.JedisCluster

abstract class MyRedisRichMapper[IN] extends RedisMapper[IN] with Serializable {

  def handlFunction(input:IN,jedisCluster: JedisCluster):Unit

}
