package td.olap.computer.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

public class RedisDBHandler extends DBHandler {

	private static Logger logger = LoggerFactory.getLogger(RedisDBHandler.class);

	private JedisPool pool;
	private Jedis redis;
	private JedisPoolConfig conf;
	private String host;
	private int port;
	private int timeout;

	public RedisDBHandler() {
		conf = new JedisPoolConfig();
		conf.setTestOnBorrow(true);
		host = "localhost";
		port = 6379;
		this.timeout = 1000;
	}

	public RedisDBHandler(String host, int port) {
		conf = new JedisPoolConfig();
		conf.setTestOnBorrow(true);
		this.host = host;
		this.port = port;
		this.timeout = 1000;
	}

	public RedisDBHandler(String host, int port, int timeout) {
		conf = new JedisPoolConfig();
		conf.setTestOnBorrow(true);
		this.host = host;
		this.port = port;
		this.timeout = timeout;
	}

	@Override
	public void open() {
		pool = new JedisPool(conf, host, port, timeout);
	}

	@Override
	public void close() {
		pool.destroy();
		pool.close();
	}

	@Override
	public void setKey(String key, String value) {
		redis = pool.getResource();
		try {
			redis.set(key, value);
		} catch (JedisException je) {
			logger.error("Error when set K:" + key + "-V:" + value, je);
			je.printStackTrace();
			pool.returnBrokenResource(redis);
			redis = null;
		} finally {
			try {
				if (redis != null)
					pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
	}

	@Override
	public void setKey(String key, byte[] value) {
		redis = pool.getResource();
		try {
			redis.set(SafeEncoder.encode(key), value);
		} catch (JedisException je) {
			logger.error("Error when set K:" + key + "-V:" + value, je);
			je.printStackTrace();
			pool.returnBrokenResource(redis);
			redis = null;
		} finally {
			try {
				if (redis != null)
					pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
	}

	@Override
	public void delKey(String key) {
		redis = pool.getResource();
		try {
			redis.del(key);
		} catch (JedisException je) {
			logger.error("Error when delete K:" + key, je);
			je.printStackTrace();
			pool.returnBrokenResource(redis);
			redis = null;
		} finally {
			try {
				if (redis != null)
					pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
	}

	@Override
	public String getStringValue(String key) throws Exception {
		String v = null;
		redis = pool.getResource();
		try {
			v = redis.get(key);
		} catch (JedisException je) {
			logger.error("Error when get K:" + key, je);
			je.printStackTrace();
			pool.returnBrokenResource(redis);
			redis = null;
		} finally {
			try {
				if (redis != null)
					pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
		return v;
	}

	@Override
	public byte[] getByteWiseValue(String key) throws Exception {
		byte[] b = null;
		redis = pool.getResource();
		try {
			b = redis.get(SafeEncoder.encode(key));
		} catch (JedisException je) {
			logger.error("Error when get K:" + key, je);
			je.printStackTrace();
			pool.returnBrokenResource(redis);
			redis = null;
		} finally {
			try {
				if (redis != null)
					pool.returnResource(redis);
			} catch (Exception e) {
				pool.returnBrokenResource(redis);
			}
		}
		return b;
	}

}
