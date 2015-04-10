package td.olap.computer.bolt;

import java.util.HashMap;

import td.olap.compute.number.TDNumber;
import td.olap.compute.number.TDNumberImpl;
import td.olap.computer.data.EmitItem;
import td.olap.computer.data.Tuple;
import td.olap.computer.mode.Bolt;

public class CountBolt extends Bolt {

	private HashMap<String, TDNumber> map;

	@Override
	public void prepare(Object... parameters) throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Override
	public int execute(EmitItem item) {
		map = new HashMap<String, TDNumber>();
		Tuple<String, String>[] msgs = (Tuple<String, String>[]) item.getMessage();
		for (Tuple<String, String> msg : msgs) {
			add(msg);
		}
		emit(item.getXid(), map);
		return map.size();
	}

	private void add(Tuple<String, String> msg) {
		String key = msg.getField();
		TDNumber value = new TDNumberImpl(msg.getMessage(0));
		if (map.containsKey(key)) {
			map.get(key).add(value);
		} else {
			map.put(key, value);
		}
	}

	@Override
	public void shutdown() {
	}

}
