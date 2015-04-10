package td.olap.computer.bolt;

import td.olap.computer.data.EmitItem;
import td.olap.computer.data.Tuple;
import td.olap.computer.mode.Bolt;

public class SplitBolt extends Bolt {

	private String splitter = "\t";

	@Override
	public void prepare(Object... parameters) throws Exception {
		// you can set the custom splitter here
	}

	/**
	 * 将spout发送的数据split成字段，并且重组成tuple类型的KV对发送出去
	 */
	@Override
	public int execute(EmitItem item) {
		String mess = (String) item.getMessage(0);
		String[] messArray = mess.split(splitter);
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < messArray.length - 1; i++) {
			sb.append(messArray[i]).append(JoinBolt.spliter);
		}
		sb.replace(sb.length() - JoinBolt.spliter.length(), sb.length(), "");
		Tuple<String, String> t = new Tuple<String, String>(sb.toString(), messArray[messArray.length - 1]);
		emit(item.getXid(), t);
		return 1;
	}

	@Override
	public void shutdown() {
	}

}
