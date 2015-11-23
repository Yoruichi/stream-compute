package td.olap.computer.mode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by apple on 15/11/12.
 */
public abstract class BucketSpout extends Spout {

    private final BlockingQueue<Serializable> queue;

    private Thread t;

    private boolean running;

    private int bucketSize;

    private int curSize;

    private int waitSec;

    private Thread createThread() {
        return new Thread(new Runnable() {
            private long start = System.currentTimeMillis();
            private List<Serializable> list = new ArrayList<Serializable>();

            public void run() {
                running = true;
                while (this.list.size() < bucketSize ||
                        (System.currentTimeMillis() - this.start) < waitSec * 1000) {
                    if (!queue.isEmpty()) {
                        try {
                            Serializable msg = queue.take();
                            list.add(msg);
                        } catch (InterruptedException e) {
                            running = false;
                            e.printStackTrace();
                        }
                    }
                    if (this.list.size() >= bucketSize ||
                            (System.currentTimeMillis() - this.start) >= waitSec * 1000) {
                        if (this.list.size() > 0) {
                            emit(this.list.toArray(new Serializable[this.list.size()]));
                            logger.info("Emit %d messages in one bucket use time %d ms.", list.size(), (System.currentTimeMillis() - this.start));
                        }
                        this.list = new ArrayList<Serializable>();
                        this.start = System.currentTimeMillis();
                        curSize = 0;
                    }
                }
            }
        });
    }

    /**
     * This will create a blocking queue with max size same with the bucket size<br>
     * you given.And this will create a thread to take message from the queue and check<br>
     * message number or wait time in a loop.<br>
     * The blocking queue has a same size with bucket size,because one bucket has one xid,<br>
     * one bolt
     *
     * @param bucketSize
     * @param waitSec
     */
    public BucketSpout(final int bucketSize, final int waitSec) {
        this.queue = new LinkedBlockingDeque<Serializable>(bucketSize);
        this.bucketSize = bucketSize;
        this.curSize = 0;
        this.waitSec = waitSec;
        this.t = createThread();
        this.t.start();
    }

    public BucketSpout() {
        this(1000, 30);
    }

    public void bucket(Serializable msg) {
        curSize++;
        this.persist(msg);
        try {
            this.queue.put(msg);
            while (curSize >= bucketSize) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
