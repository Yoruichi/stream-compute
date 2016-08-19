package td.olap.computer.consist;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import td.olap.computer.persist.DBHandler;

public class XidManager {

    public static Map<String, AtomicLong> xidMap = new ConcurrentHashMap<String, AtomicLong>();

    private static Logger logger = LoggerFactory.getLogger(XidManager.class);

    public static void registTopology(DBHandler dbHandler, String name) {
        if (dbHandler != null)
            try {
                String sXid = dbHandler.getStringValue(name + ":xid");
                long current =
                        sXid == null ? 0 : Long.valueOf(sXid) - getSpoutSize(dbHandler, name) + 1;
                xidMap.put(name, new AtomicLong(current + 1));
                dbHandler.setKey(name + ":xid", "" + (current + 1));
            } catch (Exception e) {
                logger.error("Regist topology " + name + " failed.", e);
                e.printStackTrace();
            }
    }

    public static void setSpoutSize(DBHandler dbHandler, String name, int size) {
        if (dbHandler != null)
            dbHandler.setKey(name + ":spout:size", "" + size);
    }

    public static int getSpoutSize(DBHandler dbHandler, String name) {
        String sLastSpoutSize = null;
        if (dbHandler != null)
            try {
                sLastSpoutSize = dbHandler.getStringValue(name + ":spout:size");
            } catch (Exception e) {
            }
        return sLastSpoutSize == null ? 1 : Integer.valueOf(sLastSpoutSize);
    }

    public static long getAndAdd(String topoName, long delta) {
        AtomicLong l = xidMap.get(topoName);
        if (l == null) {
            l = new AtomicLong(0);
            xidMap.put(topoName, l);
        }
        long current = l.getAndAdd(delta);
        return current;
    }

    public static void saveCurrent(DBHandler dbHandler, String topoName, long current) {
        if (dbHandler != null)
            try {
                dbHandler.setKey(topoName + ":xid", "" + current);
            } catch (Exception e) {
                logger.error("Topology " + topoName + " persist xid failed.", e);
                e.printStackTrace();
            }
    }

    public static long getAndAddAndSave(DBHandler dbHandler, String topoName, long delta) {
        AtomicLong l = xidMap.get(topoName);
        if (l == null) {
            l = new AtomicLong(0);
            xidMap.put(topoName, l);
        }
        long current = l.getAndAdd(delta);
        if (dbHandler != null)
            try {
                dbHandler.setKey(topoName + ":xid", "" + current);
            } catch (Exception e) {
                logger.error("Topology " + topoName + " persist xid failed.", e);
                e.printStackTrace();
            }
        return current;
    }

    public static long getCurrent(DBHandler dbHandler, String name) {
        String sXid = null;
        if (dbHandler != null)
            try {
                sXid = dbHandler.getStringValue(name + ":xid");
            } catch (Exception e) {
                logger.error("Topology " + name + " persist xid failed.", e);
                e.printStackTrace();
            }
        long current = sXid == null ? 0 : Long.valueOf(sXid);
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
