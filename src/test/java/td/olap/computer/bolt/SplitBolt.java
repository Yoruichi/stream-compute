package td.olap.computer.bolt;

import td.olap.computer.data.EmitItem;
import td.olap.computer.mode.Bolt;

public class SplitBolt extends Bolt {

	private String splitter="\n";
	
	@Override
	public void prepare(Object... parameters) throws Exception {
		//you can set the custom splitter here
	}

	@Override
	public int execute(EmitItem item) {
		String mess = (String) item.getMessage(0);
		String[] messArray = mess.split(splitter);
		emit(item.getXid(), messArray);
		return 1;
	}

	@Override
	public void shutdown() {
	}

}
