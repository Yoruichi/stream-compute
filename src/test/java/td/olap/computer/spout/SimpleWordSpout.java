package td.olap.computer.spout;

import td.olap.computer.mode.Spout;

public class SimpleWordSpout extends Spout {

	@Override
	public void prepare(Object... parameters) throws Exception {
	}

	@Override
	public int execute() {
		while (true) {
			String message = Thread.currentThread().getName() + ":" + Math.abs(Math.floor(Math.random() * 99 + 1));
			persist(message);
			emit(message);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void shutdown() {
	}

}
