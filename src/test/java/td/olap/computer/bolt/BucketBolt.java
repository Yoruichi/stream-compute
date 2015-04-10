package td.olap.computer.bolt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Bolt;

public class BucketBolt<T extends Serializable> extends Bolt {

	private int size = 10000;// default bucket size
	private long wait = 180;// default max wait seconds

	private List<T> list = new ArrayList<T>();
	private long start = System.currentTimeMillis();

	@Override
	public void prepare(Object... parameters) throws Exception {
		if (parameters.length > 0)
			size = (Integer) parameters[0];
		if (parameters.length > 1)
			wait = (Long) parameters[1];
	}

	@SuppressWarnings("unchecked")
	@Override
	public int execute(EmitItem item) {
		T msg = (T) item.getMessage(0);
		list.add(msg);
		int currentNum = list.size();
		if (currentNum >= size || (System.currentTimeMillis() - start) >= (wait * 1000)) {
			emit(item.getXid(), list.toArray(new Serializable[currentNum]));
			list.clear();
			start = System.currentTimeMillis();
		}
		return currentNum;
	}

	@Override
	public void shutdown() {
		// do something to provide your message safe
	}

}
