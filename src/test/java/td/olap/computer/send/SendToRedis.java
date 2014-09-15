package td.olap.computer.send;

import java.util.UUID;

import td.olap.computer.config.Config;
import td.redis.sentinel.client.RedisClient;
import td.redis.sentinel.client.component.Sentinel;

public class SendToRedis {

	public static void main(String[] args) {
		Sentinel sentinel = new Sentinel(Config.REDIS_MASTER_NAME,
				Config.SENTINEL_ADDRESS);
		RedisClient client = new RedisClient(sentinel);
		while (true) {
			client.lpush(Config.REDIS_KEY, UUID.randomUUID().toString());
		}
	}

}
