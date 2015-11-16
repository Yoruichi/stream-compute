package td.olap.computer.mode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import td.olap.computer.consist.XidManager;
import td.olap.computer.data.EmitItem;
import td.olap.computer.persist.DBHandler;
import td.olap.computer.persist.LevelDBHandler;
import td.olap.computer.persist.RedisDBHandler;
import td.olap.computer.util.Util;

/**
 * @author yoruichi
 *         <p/>
 *         Topology is a structure of spouts and bolts with some queue.<br>
 *         Topology has an array of spout,and has an execute service.all of
 *         spouts will be called by the service.<br>
 *         All of the bolts will put in a List with multi level,and every level
 *         is an array of bolt.And for every level bolt array,there is an
 *         execute service.<br>
 *         In addition,is a list of BlockingQueue.To connect spout and bolt or
 *         bolt and bolt.<br>
 */
public class Topology {
    protected final static Logger logger = LoggerFactory.getLogger(Topology.class);

    public String name;

    private int maxMissing = Integer.MAX_VALUE;

    public List<BlockingQueue<EmitItem>> messageQueueList;

    public Spout[] spoutGroup;

    public ExecutorService spoutService;

    public List<Bolt[]> boltGroupList;

    public List<ExecutorService> boltServiceList;

    public long waitTime = 1000l;

    public AtomicBoolean running = new AtomicBoolean(false);

    private DBHandler dbHandler;

    private Object[] parameters;

    public Topology(String name, DBHandler db) {
        this.name = name;
        this.dbHandler = db;
        init();
    }

    public Topology(String name) {
        this(name, null);
    }

    public Topology() {
        this("default_" + System.currentTimeMillis());
    }

    public void init() {
        messageQueueList = new ArrayList<BlockingQueue<EmitItem>>();
        boltGroupList = new ArrayList<Bolt[]>();
        boltServiceList = new ArrayList<ExecutorService>();
        try {
            if (this.dbHandler != null) {
                this.getDbHandler().open();
                logger.info("DB handler[" + this.getDbHandler().getClass().getName() + "] open succeed.");
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error when open db handler", e);
        }
        XidManager.registTopology(getDbHandler(), name);
    }

    /**
     * To set spout in this topology and clone it<br>
     *
     * @param spout
     * @param spoutNum
     * @return this instance
     */
    public Topology setSpout(Spout spout, int spoutNum) {
        BlockingQueue<EmitItem> messageQueue_1 = new LinkedBlockingQueue<EmitItem>(maxMissing);
        messageQueueList.add(messageQueue_1);

        spout.setSendMessageQueue(messageQueue_1);
        spout.setTopologyName(name);
        spoutService = Executors.newFixedThreadPool(spoutNum);

        spoutGroup = new Spout[spoutNum];

        spoutGroup[0] = spout;

        for (int i = 1; i < spoutNum; i++) {
            spoutGroup[i] = spout.clone();
            spoutGroup[i].setDbHandler(getDbHandler());
            spoutGroup[i].setCurrentXid(XidManager.getAndAdd(name, 1));
        }
        spoutGroup[0].setDbHandler(getDbHandler());
        spoutGroup[0].setCurrentXid(XidManager.getAndAdd(name, 1));
        XidManager.setSpoutSize(dbHandler, name, spoutNum);
        return this;
    }

    /**
     * To set bolt in this topology and clone it<br>
     *
     * @param bolt
     * @param boltNum
     * @return this instance
     */
    public Topology setBolt(Bolt bolt, int boltNum) {
        BlockingQueue<EmitItem> messageQueue_1 = messageQueueList.get(messageQueueList.size() - 1);
        BlockingQueue<EmitItem> messageQueue_2 = new LinkedBlockingQueue<EmitItem>(maxMissing);
        messageQueueList.add(messageQueue_2);
        bolt.setTopologyName(name);
        bolt.setReadMessageQueue(messageQueue_1);
        bolt.setSendMessageQueue(messageQueue_2);

        ExecutorService es_1 = Executors.newFixedThreadPool(boltNum);
        boltServiceList.add(es_1);

        Bolt[] boltGroup = new Bolt[boltNum];

        boltGroup[0] = bolt;
        for (int i = 1; i < boltNum; i++) {
            boltGroup[i] = bolt.clone();
            boltGroup[i].setDbHandler(getDbHandler());
        }
        bolt.setDbHandler(getDbHandler());
        boltGroupList.add(boltGroup);
        return this;
    }

    /**
     * To prepare this topology before it running.<br>
     * If you have some parameters to initialized,you should rewrite this
     * method.<br>
     * Note it,you need to call every spout's prepare method and bolt's prepare
     * method.
     *
     * @throws Exception
     */
    public void prepare() throws Exception {
        for (Spout spout : spoutGroup) {
            spout.prepare(parameters);
        }
        for (Bolt[] group : boltGroupList)
            for (Bolt bolt : group)
                bolt.prepare(parameters);
    }

    /**
     * Reload the missing task last running and make sure the bucket valid.
     */
    public void reload() {
        try {
            String sLastSucc = getDbHandler().getStringValue(name + ":lastsucc");
            long lastSucc = sLastSucc == null ? -1 : Long.valueOf(sLastSucc);
            logger.info("Last success xid is " + lastSucc + ", now ready to load un-finish task.");
            long reloadSize = XidManager.getCurrent(dbHandler, name);
            if (reloadSize > 0) {
                for (long i = (lastSucc + 1); i < reloadSize; i++) {
                    String sPackageId = getDbHandler().getStringValue(name + ":" + i);
                    int packageId = sPackageId == null ? -1 : Integer.valueOf(sPackageId);
                    logger.info("Reload the Xid " + i + " package 0 to " + packageId + ".");
                    List<Serializable> l = new ArrayList<Serializable>();
                    for (int j = 0; j <= packageId; j++) {
                        Serializable s = Util.ByteToObject(getDbHandler().getByteWiseValue(name + ":" + i + ":" + j));
                        if (s != null)
                            l.add(s);
                    }
                    if (l.size() > 0)
                        messageQueueList.get(0).put(new EmitItem(i, l.toArray(new Serializable[l.size()])));
                }
            }
            logger.info("Reload un-finish task ok.");
        } catch (Exception e) {
            logger.error("Topology " + name + " do last task failed.", e);
            e.printStackTrace();
        }
    }

    /**
     * Topology will be running.<br>
     * If this instance has already been running,there will be a message to tell
     * you in log file.<br>
     * When it running,will set every spout and bolt to NOT finish status.And
     * set every bolt's previous bolt NOT finish.If some one is NULL, will log
     * an clone error.<br>
     * And then,spout service and bolt service will submit every spout and bolt
     * in a new thread.This topology will sleep wait-time u given until every
     * spout and bolt finished.<br>
     */
    public void start() {
        if (!running.getAndSet(true)) {
            for (int i = 0; i < spoutGroup.length; i++) {
                if (spoutGroup[i] != null)
                    spoutGroup[i].setFinish(false);
                else
                    logger.error("Spout clone error.Index of [" + i + "]");
            }

            for (int i = 0; i < boltGroupList.size(); i++) {
                for (int j = 0; j < boltGroupList.get(i).length; j++) {
                    if (boltGroupList.get(i)[j] != null) {
                        boltGroupList.get(i)[j].setFinish(false);
                        boltGroupList.get(i)[j].setPrevFinish(false);
                    } else
                        logger.error("Bolt clone error.Index of [" + i + "]");
                }
            }

            for (int i = 0; i < spoutGroup.length; i++) {
                if (spoutGroup[i] != null)
                    spoutService.submit(spoutGroup[i]);
                else
                    logger.error("Spout clone error.Index of [" + i + "]");
            }
            for (int i = 0; i < boltGroupList.size(); i++) {
                for (int j = 0; j < boltGroupList.get(i).length; j++) {
                    if (boltGroupList.get(i)[j] != null)
                        boltServiceList.get(i).submit(boltGroupList.get(i)[j]);
                    else
                        logger.error("Bolt clone error.Index of [" + i + "]");
                }
            }

            while (!spoutFinish()) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (int i = 0; i < boltGroupList.size(); i++)
                while (!boltFinish(i)) {
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            running.set(false);
        } else {
            logger.info("Topology named " + name + "is already running now.");
        }
    }

    /**
     * When every spout finished,we call spout finish in topology.And set the
     * the every bolt previous finish in the head of bolt list.
     *
     * @return
     */
    public boolean spoutFinish() {
        for (int i = 0; i < spoutGroup.length; i++) {
            if (!spoutGroup[i].isFinish()) {
                logger.debug("spout group index [" + i + "] is not finish.");
                return false;
            }
        }
        if (boltGroupList.size() > 0)
            if (boltGroupList.get(0) != null) {
                for (int i = 0; i < boltGroupList.get(0).length; i++) {
                    boltGroupList.get(0)[i].setPrevFinish(true);
                }
            }
        return true;
    }

    /**
     * When every bolt in the INDEX given level of bolt list has been
     * finished,we call bolt finished in topology.
     *
     * @param index
     * @return
     */
    public boolean boltFinish(int index) {
        if (!messageQueueList.get(index).isEmpty())
            return false;
        for (int i = 0; i < boltGroupList.get(index).length; i++) {
            if (!boltGroupList.get(index)[i].isFinish()) {
                logger.debug("bolt group index [" + index + "]:[" + i + "] is not finish and prevFinish:["
                        + boltGroupList.get(index)[i].isPrevFinish() + "] and read message queue is empty:["
                        + boltGroupList.get(index)[i].getReadMessageQueue().isEmpty() + "]");
                return false;
            }
        }
        if (index < boltGroupList.size() - 1)
            if (boltGroupList.get(index + 1) != null) {
                for (int i = 0; i < boltGroupList.get(index + 1).length; i++) {
                    boltGroupList.get(index + 1)[i].setPrevFinish(true);
                }
            }
        return true;
    }

    /**
     * To get the number of spout has emitted.
     *
     * @return
     */
    public int getSpoutProcessMessageNumber() {
        int num = 0;
        if (spoutGroup != null)
            for (Spout fmh : spoutGroup) {
                num += fmh.getNum();
            }
        return num;
    }

    /**
     * To get the number of the index given level of the bolt list has executed.
     *
     * @param index
     * @return
     */
    public int getBoltProcessMessageNumber(int index) {
        int num = 0;
        if (boltGroupList.get(index) != null)
            for (Bolt mh : boltGroupList.get(index)) {
                num += mh.getNum();
            }
        return num;
    }

    /**
     * To show how many items in every queue.
     */
    public void showStats() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (running.get()) {
            for (int i = 0; i < messageQueueList.size(); i++) {
                logger.info("Index[" + i + "] queue items : " + messageQueueList.get(i).size());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Topology named " + name + " is stopped now.");
    }

    public void shutdown() {
        for (int i = 0; i < messageQueueList.size(); i++)
            messageQueueList.get(i).clear();
        for (int i = 0; i < spoutGroup.length; i++)
            spoutGroup[i].shutdown();
        spoutService.shutdown();
        for (int i = 0; i < boltGroupList.size(); i++)
            for (int j = 0; j < boltGroupList.get(i).length; j++)
                boltGroupList.get(i)[j].shutdown();
        for (int i = 0; i < boltServiceList.size(); i++)
            boltServiceList.get(i).shutdown();
        try {
            if (this.dbHandler != null) {
                getDbHandler().close();
            }
        } catch (Exception e) {
            new RuntimeException("Error when close db handler.", e);
        }
    }

    public long getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    public int getMaxMissing() {
        return maxMissing;
    }

    public void setMaxMissing(int maxMissing) {
        this.maxMissing = maxMissing;
    }

    public DBHandler getDbHandler() {
        return dbHandler;
    }

    public void setDbHandler(DBHandler dbHandler) {
        this.dbHandler = dbHandler;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

}