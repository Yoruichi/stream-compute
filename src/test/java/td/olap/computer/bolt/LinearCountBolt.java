package td.olap.computer.bolt;

import td.olap.computer.config.Config;
import td.olap.computer.data.EmitItem;
import td.olap.computer.linearcount.LinearHandler;
import td.olap.computer.mode.Bolt;
import td.redis.sentinel.client.RedisClient;
import td.redis.sentinel.client.component.Sentinel;

public class LinearCountBolt extends Bolt {

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
		LinearHandler linearHandler = new LinearHandler();
		Object[] message = item.getMessage();
		for (Object object : message) {
			linearHandler.offer(object);
		}
		long res = linearHandler.getCount();
		client.hset("batch_result", "" + item.getXid(), res + "");
		logger.info(Thread.currentThread().getName() + " count Xid-"
				+ item.getXid() + " item, num is " + res);
		emit(item.getXid(), linearHandler);
		return message.length;
	}

	@Override
	public void shutdown() {
	}

}
