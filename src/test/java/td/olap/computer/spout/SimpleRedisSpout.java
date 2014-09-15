package td.olap.computer.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import td.olap.computer.config.Config;
import td.olap.computer.consist.XidManager;
import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Spout;
import td.olap.computer.persist.LevelDBHandler;
import td.redis.sentinel.client.RedisClient;
import td.redis.sentinel.client.component.Sentinel;

public class SimpleRedisSpout extends Spout {

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
	public int execute() {
		List<String> list = new ArrayList<String>();
		long start = System.currentTimeMillis();
		while (true) {
			String message = client.rpop(Config.REDIS_KEY);
			if (message != null) {
				persist(message);
				list.add(message);
			}
			if (list.size() >= 10000
					|| (list.size() > 0 && (System.currentTimeMillis() - start) > 3 * 1000)) {
				emit(list.toArray());
				logger.info(Thread.currentThread().getName() + " send "
						+ list.size() + " items with Xid-" + (getCurrentXid()-1));
				list = new ArrayList<String>();
				start = System.currentTimeMillis();
			}
		}
	}

	@Override
	public void shutdown() {
	}

	public static void main(String[] args) {
		BlockingQueue<EmitItem> q = new LinkedBlockingQueue<EmitItem>(10);
		SimpleRedisSpout sp = new SimpleRedisSpout();
		sp.setTopologyName("test_persist");
		sp.setSendMessageQueue(q);
		LevelDBHandler db = new LevelDBHandler(sp.getTopologyName());
		db.open();
		sp.setDbHandler(db);
		XidManager.registTopology(db, sp.getTopologyName());
		try {
			sp.prepare(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		sp.execute();
	}

}
