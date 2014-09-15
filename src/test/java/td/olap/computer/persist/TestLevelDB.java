package td.olap.computer.persist;


public class TestLevelDB {

	public static void main(String[] args) throws Exception {
		String topoName = "LinearCountTopology";
		LevelDBHandler db = new LevelDBHandler(topoName);
		db.open();
		System.out.println(db.getStringValue("xid"));
		System.out.println(db.getStringValue("lastsucc"));
		System.out.println(db.getStringValue("5"));
		System.out.println(db.getStringValue("1:6842"));
		db.close();
		// Topology topo = new Topology(topoName);
		// topo.beforeStart();
	}

}
