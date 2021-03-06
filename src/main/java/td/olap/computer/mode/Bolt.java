package td.olap.computer.mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import td.olap.computer.data.EmitItem;
import td.olap.computer.persist.DBHandler;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yoruichi
 *         <p/>
 *         Bolt is a mode which could do your business in a topology.<br>
 *         It has two queue clients.One is for reading,and the other is for
 *         emitting.<br>
 *         Bolt will be running as a thread too.The execute method could finish
 *         your business if you rewrite.<br>
 *         The last bolt in a topology also has a send message queue,but it
 *         could not send anything.
 */
public abstract class Bolt implements Runnable, Cloneable {

    protected final static Logger logger = LoggerFactory.getLogger(Bolt.class);

    private BlockingQueue<EmitItem> readMessageQueue;
    private BlockingQueue<EmitItem> sendMessageQueue;
    public AtomicBoolean prevFinish = new AtomicBoolean(false);
    private AtomicBoolean finish = new AtomicBoolean(false);
    private int num;
    private String topologyName;
    private DBHandler dbHandler;

    public Bolt() {

    }

    public Bolt(BlockingQueue<EmitItem> readMessageQueue, BlockingQueue<EmitItem> sendMessageQueue) {
        this.setReadMessageQueue(readMessageQueue);
        this.setSendMessageQueue(sendMessageQueue);
    }

    /**
     * Initialized something you need.This method will be called once when
     * topology start.
     *
     * @param parameters
     * @throws Exception
     */
    public abstract void prepare(Object... parameters) throws Exception;

    /**
     * When the previous bolt/spout is not finished, or the queue for reading is
     * not empty,this method will be called.<br>
     * A item will be loaded from reading queue,and the item will be computed in
     * the execute method.
     */
    public void run() {
        num = 0;
        while (!isPrevFinish() || !getReadMessageQueue().isEmpty()) {
            EmitItem item = null;
            try {
                logger.debug(Thread.currentThread().getName() + " take a item from queue.Maybe blocking.");
                item = getReadMessageQueue().take();
            } catch (InterruptedException e) {
                logger.error("Queue server has problem");
                e.printStackTrace();
            }
            if (item != null) {
                logger.debug(Thread.currentThread().getName() + " execute the item id " + item.getXid());
                num = num + execute(item);
            }
        }
        setFinish(true);
    }

    /**
     * Do your business.
     *
     * @param item
     * @return
     */
    public abstract int execute(EmitItem item);

    /**
     * In this method,will put a item into the blocking queue.If the queue is
     * full,this thread will be blocking.
     *
     * @param index
     * @param messages
     */
    public void emit(long index, Serializable... messages) {
        if (messages == null) {
            logger.debug(Thread.currentThread().getName() + " emit nothing.");
            return;
        }
        EmitItem item = new EmitItem(index, messages);
        try {
            logger.debug(Thread.currentThread().getName() + " emit messages with index " + index + " .Maybe blocking.");
            getSendMessageQueue().put(item);
        } catch (InterruptedException e) {
            logger.error("Queue server has problem");
            e.printStackTrace();
        }
    }

    public void commit(long xid) {
        if (getDbHandler() == null) {
            return;
        }
        try {
            String pid = getDbHandler().hGetStringValue(topologyName, "" + xid);
            long packageId = pid == null ? -1 : Long.valueOf(pid);
            for (int i = 0; i <= packageId; i++) {
                getDbHandler().delKey(topologyName + ":" + xid + ":" + i);
            }
            this.dbHandler.hDelField(topologyName, "" + xid);
        } catch (Exception e) {
            logger.error("Commit message from topology " + topologyName + " failed. Xid " + xid + ".", e);
            e.printStackTrace();
        }
    }

    public boolean isPrevFinish() {
        return prevFinish.get();
    }

    public void setPrevFinish(boolean prevFinish) {
        this.prevFinish.set(prevFinish);
    }

    @Override
    public Bolt clone() {
        try {
            return (Bolt) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean isFinish() {
        return finish.get();
    }

    public abstract void shutdown();

    public BlockingQueue<EmitItem> getReadMessageQueue() {
        return readMessageQueue;
    }

    public void setReadMessageQueue(BlockingQueue<EmitItem> readMessageQueue) {
        this.readMessageQueue = readMessageQueue;
    }

    public BlockingQueue<EmitItem> getSendMessageQueue() {
        return sendMessageQueue;
    }

    public void setSendMessageQueue(BlockingQueue<EmitItem> sendMessageQueue) {
        this.sendMessageQueue = sendMessageQueue;
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

    public DBHandler getDbHandler() {
        return dbHandler;
    }

    public void setDbHandler(DBHandler dbHandler) {
        this.dbHandler = dbHandler;
    }

}