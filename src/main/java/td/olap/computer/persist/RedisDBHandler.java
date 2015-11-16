package td.olap.computer.persist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

import java.util.Set;

public class RedisDBHandler extends DBHandler {

    private static Logger logger = LoggerFactory.getLogger(RedisDBHandler.class);

    private JedisPool pool;
    private JedisPoolConfig conf;
    private String host;
    private int port;
    private int timeout;

    public RedisDBHandler() {
        this("localhost", 6379);
    }

    public RedisDBHandler(String host, int port) {
        this(host, port, 1000);
    }

    public RedisDBHandler(String host, int port, int timeout) {
        conf = new JedisPoolConfig();
        conf.setTestOnBorrow(true);
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }

    public RedisDBHandler(JedisPoolConfig conf, String host, int port, int timeout) {
        this.conf = conf;
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
        Jedis redis = pool.getResource();
        try {
            redis.set(key, value);
        } catch (JedisException je) {
            logger.error("Error when set K:" + key + "-V:" + value, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
    }

    @Override
    public void setKey(String key, byte[] value) {
        Jedis redis = pool.getResource();
        try {
            redis.set(SafeEncoder.encode(key), value);
        } catch (JedisException je) {
            logger.error("Error when set K:" + key + "-V:" + value, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
    }

    @Override
    public void hSetKey(String key, String field, String value) {
        Jedis redis = pool.getResource();
        try {
            redis.hset(key, field, value);
        } catch (JedisException je) {
            logger.error("Error when set K:" + key + "-F:" + field + "-V:" + value, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
    }

    @Override
    public void hSetKey(String key, String field, byte[] value) {
        Jedis redis = pool.getResource();
        try {
            redis.hset(SafeEncoder.encode(key), SafeEncoder.encode(field), value);
        } catch (JedisException je) {
            logger.error("Error when set K:" + key + "-F:" + field + "-V:" + value, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
    }

    @Override
    public String hGetStringValue(String key, String field) {
        String v = null;
        Jedis redis = pool.getResource();
        try {
            v = redis.hget(key, field);
        } catch (JedisException je) {
            logger.error("Error when get K:" + key + "-F:" + field + "-V:", je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
        return v;
    }

    @Override
    public byte[] hGetWiseValue(String key, String field) {
        byte[] v = null;
        Jedis redis = pool.getResource();
        try {
            v = redis.hget(SafeEncoder.encode(key), SafeEncoder.encode(field));
        } catch (JedisException je) {
            logger.error("Error when get K:" + key + "-F:" + field + "-V:", je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
        return v;
    }

    @Override
    public void hDelField(String key, String field) {
        Jedis redis = pool.getResource();
        try {
            redis.hdel(key, field);
        } catch (JedisException je) {
            logger.error("Error when get K:" + key + "-F:" + field + "-V:", je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
    }

    @Override
    public Set<String> hGetFields(String key) {
        Set<String> fields = null;
        Jedis redis = pool.getResource();
        try {
            fields = redis.hkeys(key);
        } catch (JedisException je) {
            logger.error("Error when get fields by K:" + key, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
        return fields;
    }

    @Override
    public void delKey(String key) {
        Jedis redis = pool.getResource();
        try {
            redis.del(key);
        } catch (JedisException je) {
            logger.error("Error when delete K:" + key, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
    }

    @Override
    public String getStringValue(String key) throws Exception {
        String v = null;
        Jedis redis = pool.getResource();
        try {
            v = redis.get(key);
        } catch (JedisException je) {
            logger.error("Error when get K:" + key, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
        return v;
    }

    @Override
    public byte[] getByteWiseValue(String key) throws Exception {
        byte[] b = null;
        Jedis redis = pool.getResource();
        try {
            b = redis.get(SafeEncoder.encode(key));
        } catch (JedisException je) {
            logger.error("Error when get K:" + key, je);
            je.printStackTrace();
        } finally {
            redis.close();
        }
        return b;
    }

}
