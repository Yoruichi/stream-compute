package td.olap.computer.bolt;

import java.util.HashMap;
import java.util.Map;

import td.olap.computer.data.EmitItem;
import td.olap.computer.data.Tuple;
import td.olap.computer.mode.Bolt;

public class JoinBolt extends Bolt {

	public static String spliter = "%qn%";
	private Map<String, String> rightSet = new HashMap<String, String>();

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Object... parameters) throws Exception {
		if (parameters.length > 1)
			rightSet = (Map<String, String>) parameters[0];
	}

	@Override
	@SuppressWarnings("unchecked")
	public int execute(EmitItem item) {
		long xid = item.getXid();
		Tuple<String, String> t = (Tuple<String, String>) item.getMessage(0);
		String k = t.getField();
		if (rightSet.containsKey(k)) {
			Tuple<String, String> nt = new Tuple<String, String>(k, t.getMessage(0), rightSet.get(k));
			emit(xid, nt);
		} else {
			emit(xid, t);
		}
		return 0;
	}

	@Override
	public void shutdown() {
		rightSet = null;
	}

}
