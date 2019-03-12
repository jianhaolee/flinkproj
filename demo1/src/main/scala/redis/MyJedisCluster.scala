package redis

import redis.clients.jedis.{HostAndPort, JedisCluster}

object MyJedisCluster extends Serializable {
  private val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6000))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6001))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6002))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6003))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6004))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6005))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6006))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6007))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6009))
  jedisClusterNodes.add(new HostAndPort("221.122.77.6", 6010))


//  val clients = new JedisCluster(jedisClusterNodes)
  private val password:String  = "vc7fiugig%5u85!"
//  new JedisCluster(jedisClusterNodes, 10000, 1000, 1, password, new GenericObjectPoolConfig)
  val jedisCluster = new JedisCluster(jedisClusterNodes)
}
