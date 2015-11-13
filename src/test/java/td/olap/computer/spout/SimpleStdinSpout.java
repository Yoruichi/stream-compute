package td.olap.computer.spout;

import java.util.Scanner;

import td.olap.computer.mode.Spout;
import td.olap.computer.persist.RedisDBHandler;

public class SimpleStdinSpout extends Spout {

	private Scanner canner;

	@Override
	public void prepare(Object... parameters) throws Exception {
	}

	@Override
	public int execute() {
		canner = new Scanner(System.in);
		while (true) {
			String message = canner.nextLine();
			if (message != null && !message.trim().equals("")) {
				persist(message);
				emit(message);
				System.out.println("message xid[" + (getCurrentXid()-1) + "] has been send.");
			}
		}
	}

	@Override
	public void shutdown() {
	}

	public static void main(String[] args) {
		SimpleStdinSpout sss = new SimpleStdinSpout();
        try {
            sss.initializedForTest(new RedisDBHandler()).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
