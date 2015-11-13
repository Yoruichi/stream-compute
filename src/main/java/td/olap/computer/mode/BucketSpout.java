package td.olap.computer.mode;

import td.olap.computer.mode.Spout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by apple on 15/11/12.
 */
public abstract class BucketSpout extends Spout {
//TODO test
    private final BlockingQueue<Serializable> queue;

    private Thread t;

    private boolean running;

    private Thread createThread(final int bucketSize, final int waitSec) {
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
                    } else {
                        try {
                            Thread.sleep(waitSec * 1000 / 4);
                        } catch (InterruptedException e) {
                            running = false;
                            e.printStackTrace();
                        }
                    }
                    if (this.list.size() >= bucketSize || (this.list.size() > 0 &&
                            (System.currentTimeMillis() - this.start) >= waitSec * 1000)) {
                        emit(this.list.toArray(new Serializable[this.list.size()]));
                        this.list = new ArrayList<Serializable>();
                        this.start = System.currentTimeMillis();
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
        this.t = createThread(bucketSize, waitSec);
        this.t.start();
    }

    public BucketSpout() {
        this(1000, 30);
    }

    public void bucket(Serializable msg) {
        this.persist(msg);
        try {
            this.queue.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
