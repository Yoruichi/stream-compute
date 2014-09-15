package td.olap.computer.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;

import td.olap.computer.mode.Spout;

public class SimpleFileSpout extends Spout {

	public String path;

	public int readTo;

	@Override
	public void prepare(Object... parameters) throws Exception {
		path = (String) parameters[0];
	}

	@Override
	public int execute() {
		String _sreadTo = null;
//		try {
//			_sreadTo = getDbHandler().getStringValue(getTopologyName() + ":" + path);
//		} catch (Exception e1) {
//			e1.printStackTrace();
//		}
		int num = 0;
		readTo = _sreadTo == null ? 0 : Integer.parseInt(_sreadTo);
		BufferedReader br = null;
		try {
			while (true) {
				int currentReadTo = 0;
				br = new BufferedReader(new FileReader(new File(path)));
				String context = null;
				while ((context = br.readLine()) != null) {
					currentReadTo++;
					if (currentReadTo > readTo) {
//						System.out.println(context);
						// persist(context);
						emit(context);
						num++;
						readTo++;
//						getDbHandler().setKey(getTopologyName() + ":" + path, "" + readTo);
					}
				}
				br.close();
				Thread.sleep(1000);
				logger.info("read nothing from path[" + path + "], read to line number " + readTo + ".");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return num;
	}

	@Override
	public void shutdown() {
	}

	public static void main(String[] args) {
		SimpleFileSpout sfs = new SimpleFileSpout();
		sfs.path = "/Users/apple/Documents/active.log";
		sfs.initializedForTest().execute();
	}

}
