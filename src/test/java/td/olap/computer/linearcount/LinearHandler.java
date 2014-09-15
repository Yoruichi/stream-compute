package td.olap.computer.linearcount;

import java.io.Serializable;
import com.clearspring.analytics.stream.cardinality.LinearCounting;

public class LinearHandler implements Serializable {

	private String name;

	private LinearCounting lc;

	private final int size;

	public LinearHandler() {
		this("default");
	}

	public LinearHandler(String name) {
		this(name, Integer.MAX_VALUE);
	}

	public LinearHandler(String name, int maxMembers) {
		setName(name);
		setLc(LinearCounting.Builder.onePercentError(maxMembers).build());
		size = lc.sizeof();
	}

	public LinearHandler(byte[] map) {
		this("default", map);
	}

	public LinearHandler(String name, byte[] map) {
		setName(name);
		if (map == null || map.length == 0) {
			setLc(LinearCounting.Builder.onePercentError(Integer.MAX_VALUE)
					.build());
		} else {
			setLc(new LinearCounting(map));
		}
		size = lc.sizeof();
	}

	public boolean offer(Object o) {
		return getLc().offer(o);
	}

	public byte[] getBytes() {
		return getLc().getBytes();
	}

	public long getCount() {
		return getLc().cardinality();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public LinearCounting getLc() {
		return lc;
	}

	public void setLc(LinearCounting lc) {
		this.lc = lc;
	}

	public int getSize() {
		return size;
	}

}
