package td.olap.computer.mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import td.olap.computer.consist.XidManager;
import td.olap.computer.data.EmitItem;
import td.olap.computer.persist.DBHandler;
import td.olap.computer.persist.RedisDBHandler;
import td.olap.computer.util.Util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yoruichi
 *         <p/>
 *         Spout means a head of a topology.It can emit items into topology with
 *         a blocking queue.<br>
 *         Spout will be running as a thread.You should rewrite the execute
 *         method to do what you want.
 */
public abstract class Spout implements Runnable, Cloneable {

    protected final static Logger logger = LoggerFactory.getLogger(Spout.class);

    private DBHandler dbHandler;

    private BlockingQueue<EmitItem> sendMessageQueue;
    private AtomicBoolean finish = new AtomicBoolean(false);
    private int num;
    private String topologyName;
    private long currentXid;
    private int packageId;

    public Spout initializedForTest() throws Exception {
        sendMessageQueue = new LinkedBlockingQueue<EmitItem>();
        topologyName = "LocalTest";
        dbHandler = new RedisDBHandler();
        dbHandler.open();
        XidManager.registTopology(dbHandler, topologyName);
        return this;
    }

    public Spout initializedForTest(DBHandler dbHandler) throws Exception {
        sendMessageQueue = new LinkedBlockingQueue<EmitItem>();
        topologyName = "LocalTest";
        setDbHandler(dbHandler);
        dbHandler.open();
        XidManager.registTopology(dbHandler, topologyName);
        return this;
    }

    /**
     * Initialized something you need.This method will be called once when
     * topology start.
     *
     * @param parameters
     * @throws Exception
     */
    public abstract void prepare(Object... parameters) throws Exception;

    public void run() {
        num = execute();
        setFinish(true);
    }

    /**
     * Do your business here.
     *
     * @return
     */
    public abstract int execute();

    /**
     * If the topology's db handler is null, this method is NOT usable
     *
     * @param message
     */
    public void persist(Serializable message) {
        if (getDbHandler() == null) {
            return;
        }
        try {
            getDbHandler().setKey(topologyName + ":" + currentXid + ":" + packageId, Util.ObjectToByte(message));
//            getDbHandler().setKey(topologyName + ":" + currentXid, "" + packageId);
            this.dbHandler.hSetKey(topologyName, "" + currentXid, "" + packageId);
            packageId++;
        } catch (Exception e) {
            logger.error("Persist messages from topology " + topologyName + " failed. xid " + currentXid + "_"
                    + packageId + " messages " + message + ".", e);
            e.printStackTrace();
        }
    }

    /**
     * In this method,will put a item into the blocking queue.If the queue is
     * full,this thread will be blocking.
     *
     * @param messages
     */
    public void emit(Serializable... messages) {
        if (messages == null) {
            logger.debug(Thread.currentThread().getName() + " emit nothing.");
            return;
        }
        EmitItem item = new EmitItem(currentXid, messages);
        try {
            logger.debug(Thread.currentThread().getName() + " emit messages with index " + currentXid
                    + " .Maybe blocking.");
            setCurrentXid(XidManager.getAndAddAndSave(dbHandler, topologyName, 1));
            getSendMessageQueue().put(item);
            this.packageId = 0;
        } catch (Exception e) {
            logger.error("Persist messages from topology " + topologyName + " failed. xid " + currentXid + " messages "
                    + Arrays.toString(messages) + ".", e);
            e.printStackTrace();
        }
    }

    public Spout clone() {
        try {
            return (Spout) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public abstract void shutdown();

    public BlockingQueue<EmitItem> getSendMessageQueue() {
        return sendMessageQueue;
    }

    public void setSendMessageQueue(BlockingQueue<EmitItem> sendMessageQueue) {
        this.sendMessageQueue = sendMessageQueue;
    }

    public boolean isFinish() {
        return finish.get();
    }

    public void setFinish(boolean finish) {
        this.finish.set(finish);
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public long getCurrentXid() {
        return currentXid;
    }

    public void setCurrentXid(long currentXid) {
        this.currentXid = currentXid;
    }

    public DBHandler getDbHandler() {
        return dbHandler;
    }

    public void setDbHandler(DBHandler dbHandler) {
        this.dbHandler = dbHandler;
    }

}
