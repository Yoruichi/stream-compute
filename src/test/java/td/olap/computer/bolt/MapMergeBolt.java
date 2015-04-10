package td.olap.computer.bolt;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import td.olap.compute.number.TDNumber;
import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Bolt;

public class MapMergeBolt extends Bolt {

	private ConcurrentHashMap<String, TDNumber> map = new ConcurrentHashMap<String, TDNumber>();

	@Override
	public void prepare(Object... parameters) throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Override
	public int execute(EmitItem item) {
		HashMap<String, TDNumber> m = (HashMap<String, TDNumber>) item.getMessage(0);
		for (String k : m.keySet()) {
			if (map.containsKey(k)) {
				map.get(k).add(m.get(k));
			} else {
				map.put(k, m.get(k));
			}
		}
		return 1;
	}

	@Override
	public void shutdown() {
	}

}
