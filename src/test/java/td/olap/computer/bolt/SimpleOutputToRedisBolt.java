package td.olap.computer.bolt;

import td.olap.computer.config.Config;
import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Bolt;
import td.redis.sentinel.client.RedisClient;
import td.redis.sentinel.client.component.Sentinel;

public class SimpleOutputToRedisBolt extends Bolt {

	private static Sentinel sentinel;
	private static RedisClient client;

	@Override
	public void prepare(Object... parameters) throws Exception {
		if (sentinel == null)
			sentinel = new Sentinel(Config.REDIS_MASTER_NAME,
					Config.SENTINEL_ADDRESS);
		if (client == null)
			client = new RedisClient(sentinel);
	}

	@Override
	public int execute(EmitItem item) {
		Object[] messages = item.getMessage();
		for (Object object : messages) {
			//TODO
		}
		return 0;
	}

	@Override
	public void shutdown() {
		sentinel.shutdown();
	}

}
