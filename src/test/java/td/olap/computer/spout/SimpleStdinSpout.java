package td.olap.computer.spout;

import java.util.Scanner;

import td.olap.computer.mode.Spout;

public class SimpleStdinSpout extends Spout {

	@Override
	public void prepare(Object... parameters) throws Exception {
	}

	@Override
	public int execute() {
		Scanner canner = new Scanner(System.in);
		while (true) {
			String message = canner.nextLine();
			if (message != null && !message.trim().equals("")) {
				persist(message);
				emit(message);
				System.out.println("message xid[" + getCurrentXid()
						+ "] has been send.");
			}
		}
	}

	@Override
	public void shutdown() {
	}

	public static void main(String[] args) {
		SimpleStdinSpout sss = new SimpleStdinSpout();
		sss.initializedForTest().execute();
	}

}
