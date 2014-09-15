package td.olap.computer.bolt;

import td.olap.computer.config.Config;
import td.olap.computer.data.EmitItem;
import td.olap.computer.linearcount.LinearHandler;
import td.olap.computer.linearcount.LinearHandlerTool;
import td.olap.computer.linearcount.MergeNotSupportException;
import td.olap.computer.mode.Bolt;
import td.redis.sentinel.client.RedisClient;
import td.redis.sentinel.client.component.Sentinel;

public class MergeBolt extends Bolt {

	private static Sentinel sentinel;
	private static RedisClient client;

	private static LinearHandler linearHandler;

	private static int standby = 0;

	@Override
	public void prepare(Object... parameters) throws Exception {
		if (sentinel == null)
			sentinel = new Sentinel(Config.REDIS_MASTER_NAME,
					Config.SENTINEL_ADDRESS);
		if (client == null)
			client = new RedisClient(sentinel);
		if (linearHandler == null)
			linearHandler = new LinearHandler(client.get("linear_result"
					.getBytes()));
	}

	@Override
	public int execute(EmitItem item) {
		try {
			linearHandler = LinearHandlerTool.merge(linearHandler,
					(LinearHandler) item.getMessage(0));
		} catch (MergeNotSupportException e) {
			e.printStackTrace();
		}
		int nowadays = (int) linearHandler.getCount();
		client.set("linear_result".getBytes(), linearHandler.getBytes());
		client.set("total_result", nowadays + "");
		client.hset("newpartment", item.getXid() + "", (nowadays - standby)
				+ "");
		logger.info(Thread.currentThread().getName() + " total count is "
				+ nowadays);
		standby = nowadays;
		return 1;
	}

	@Override
	public void shutdown() {
	}

}
