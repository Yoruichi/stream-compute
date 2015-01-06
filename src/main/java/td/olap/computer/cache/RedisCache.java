package td.olap.computer.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisCache implements Cache<String, String> {

	private Jedis redis;
	private JedisPool pool;

	public RedisCache() {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		String host = "localhost";
		int port = 6379;
		poolConfig.setMaxIdle(5);
		poolConfig.setMaxWaitMillis(1000);
		poolConfig.setTestOnBorrow(true);
		
		pool = new JedisPool(poolConfig, host, port);
	}

	@Override
	public String put(String key, String value) {
		String v = value;
		if (redis == null)
			redis = pool.getResource();
		try {
			v = redis.getSet(key, value);
			if (v == null)
				v = value;
		} catch (JedisConnectionException e) {
			e.printStackTrace();
		} finally {
			try {
				pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
		return v;
	}

	@Override
	public String get(String key) {
		String v = null;
		if (redis == null)
			redis = pool.getResource();
		try {
			v = redis.get(key);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
		} finally {
			try {
				pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
		return v;
	}

}
