package td.olap.compute.number;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.google.common.util.concurrent.AtomicDouble;

public class TDNumberImpl extends TDNumber {

	private AtomicDouble value = new AtomicDouble();

	private int scale = 19;
	
	public TDNumberImpl() {
	}

	public TDNumberImpl(int i) {
		value.addAndGet(i);
	}

	public TDNumberImpl(double d) {
		value.addAndGet(d);
	}

	public TDNumberImpl(long l) {
		value.addAndGet(l);
	}

	public TDNumberImpl(String str) {
		try {
			value.addAndGet(Double.parseDouble(str));
		} catch (Exception e) {
		}
	}

	@Override
	public String toString() {
		return value.toString();
	}

	@Override
	public void add() {
		value.addAndGet(1);
	}

	@Override
	public void add(int i) {
		value.addAndGet(i);
	}

	@Override
	public void add(long l) {
		value.addAndGet(l);
	}

	@Override
	public void add(double d) {
		value.addAndGet(d);
	}

	@Override
	public int getInt() {
		return value.intValue();
	}

	@Override
	public double getDouble() {
		return value.doubleValue();
	}

	@Override
	public long getLong() {
		return value.longValue();
	}

	public static void main(String[] args) {
		TDNumberImpl t = new TDNumberImpl(123);
		t.divide(87.6);
		System.out.println(t.getDouble());
		System.out.println(123/87.6);
	}

	@Override
	public void add(TDNumber t) {
		value.addAndGet(t.getDouble());
	}

	@Override
	public void minus() {
		value.addAndGet(-1);
	}

	@Override
	public void multiply() {
	}

	@Override
	public void divide() {
	}

	@Override
	public void minus(TDNumber t) {
		value.addAndGet(t.getDouble() * -1);
	}

	@Override
	public void multiply(TDNumber t) {
		value.set(BigDecimal.valueOf(value.doubleValue()).multiply(BigDecimal.valueOf(t.getDouble())).doubleValue());
	}

	@Override
	public void divide(TDNumber t) {
		value.set(BigDecimal.valueOf(value.doubleValue()).divide(BigDecimal.valueOf(t.getDouble()),scale,RoundingMode.HALF_DOWN).doubleValue());
	}

	@Override
	public void minus(int i) {
		value.addAndGet(-1 * i);
	}

	@Override
	public void multiply(int i) {
		value.set(BigDecimal.valueOf(value.doubleValue()).multiply(BigDecimal.valueOf(i)).doubleValue());
	}

	@Override
	public void divide(int i) {
		value.set(BigDecimal.valueOf(value.doubleValue()).divide(BigDecimal.valueOf(i),scale,RoundingMode.HALF_DOWN).doubleValue());
	}

	@Override
	public void minus(long l) {
		value.addAndGet(-1 * l);
	}

	@Override
	public void multiply(long l) {
		value.set(BigDecimal.valueOf(value.doubleValue()).multiply(BigDecimal.valueOf(l)).doubleValue());
	}

	@Override
	public void divide(long l) {
		value.set(BigDecimal.valueOf(value.doubleValue()).divide(BigDecimal.valueOf(l),scale,RoundingMode.HALF_DOWN).doubleValue());
	}

	@Override
	public void minus(double d) {
		value.addAndGet(-1 * d);
	}

	@Override
	public void multiply(double d) {
		value.set(BigDecimal.valueOf(value.doubleValue()).multiply(BigDecimal.valueOf(d)).doubleValue());
	}

	@Override
	public void divide(double d) {
		value.set(BigDecimal.valueOf(value.doubleValue()).divide(BigDecimal.valueOf(d),scale,RoundingMode.HALF_DOWN).doubleValue());
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}

}
