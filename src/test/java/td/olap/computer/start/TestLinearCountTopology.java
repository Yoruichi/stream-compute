package td.olap.computer.start;

import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Bolt;
import td.olap.computer.mode.Topology;
import td.olap.computer.persist.RedisDBHandler;
import td.olap.computer.spout.SimpleStdinSpout;
import td.olap.computer.spout.SimpleWordSpout;

public class TestLinearCountTopology {

    public static void main(String[] args) {
        final Topology topo = new Topology("LinearCountTopology", new RedisDBHandler());
        topo.setMaxMissing(100);
        topo.setSpout(new SimpleWordSpout(1000), 3).setBolt(new Bolt() {

            private int sum = 0;

            @Override
            public void prepare(Object... parameters) throws Exception {
            }

            @Override
            public int execute(EmitItem item) {
                sum++;
                System.out.println("Get message with xid [" + item.getXid() + "] length [" + item.getMessage().length + "] and sum number >>> " + sum);
                commit(item.getXid());
                return 0;
            }

            @Override
            public void shutdown() {
            }
        }, 1);
        try {
            topo.prepare();
        } catch (Exception e) {
            e.printStackTrace();
        }
        topo.reload();
        topo.start();
    }

}
