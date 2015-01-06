package td.olap.computer.consist;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import td.olap.computer.persist.DBHandler;
import td.olap.computer.persist.LevelDBHandler;

public class XidManager {

	public static Map<String, AtomicLong> xidMap = new ConcurrentHashMap<String, AtomicLong>();

	private static Logger logger = LoggerFactory.getLogger(XidManager.class);

	public static void registTopology(DBHandler dbHandler, String name) {
		try {
			String sXid = dbHandler.getStringValue("xid");
			long current = sXid == null ? 0 : Long.valueOf(sXid);
			xidMap.put(name, new AtomicLong(current));
		} catch (Exception e) {
			logger.error("Regist topology " + name + " failed.", e);
			e.printStackTrace();
		}

	}

	public static long addAndGet(DBHandler dbHandler, String topoName,
			long delta) {
		AtomicLong l = xidMap.get(topoName);
		if (l == null) {
			l = new AtomicLong(0);
			xidMap.put(topoName, l);
		}
		long current = l.addAndGet(delta);
		try {
			dbHandler.setKey("xid", "" + current);
		} catch (Exception e) {
			logger.error("Topology " + topoName + " persist xid failed.", e);
			e.printStackTrace();
		}
		return current;
	}

	public static long get(String topoName) {
		AtomicLong l = xidMap.get(topoName);
		if (l == null) {
			l = new AtomicLong(0);
			xidMap.put(topoName, l);
		}
		return l.get();
	}

}
