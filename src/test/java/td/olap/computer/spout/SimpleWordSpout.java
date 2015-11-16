package td.olap.computer.spout;

import td.olap.computer.mode.BucketSpout;

public class SimpleWordSpout extends BucketSpout {

    private int size;

    public SimpleWordSpout(int size) {
        super(100, 1);
        this.size = size;
    }

    @Override
    public void prepare(Object... parameters) throws Exception {
    }

    @Override
    public int execute() {
        int i = 0;
        while (i < size) {
//            String message = Thread.currentThread().getName() + ":" + Math.abs(Math.floor(Math.random() * 99 + 1));
            String message = Thread.currentThread().getName() + ":" + i;
            bucket(message);
//            System.out.println("send message >>> " + message);
            i++;
        }
        return size;
    }

    @Override
    public void shutdown() {
    }

}
