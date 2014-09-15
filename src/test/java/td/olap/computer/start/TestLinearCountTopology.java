package td.olap.computer.start;

import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Bolt;
import td.olap.computer.mode.Topology;
import td.olap.computer.spout.SimpleRedisSpout;

public class TestLinearCountTopology {

	public static void main(String[] args) {
		final Topology topo = new Topology("LinearCountTopology");
//		topo.setMaxMissing(10);
		topo.setSpout(new SimpleRedisSpout(), 1)
				.setBolt(new Bolt(){

					@Override
					public void prepare(Object... parameters) throws Exception {
					}

					@Override
					public int execute(EmitItem item) {
						System.out.println(item.getXid());
						commit(item.getXid());
						return 0;
					}

					@Override
					public void shutdown() {
					}}, 1);
		try {
			topo.prepare();
		} catch (Exception e) {
			e.printStackTrace();
		}
		topo.reload();
		topo.start();
	}

}
