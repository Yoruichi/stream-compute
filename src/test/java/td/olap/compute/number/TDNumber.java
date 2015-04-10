package td.olap.compute.number;

public abstract class TDNumber {

	/**
	 * constructor
	 */
	public TDNumber() {
	}

	/**
	 * constructor with parameter int i
	 * 
	 * @param i
	 */
	public TDNumber(int i) {
	}

	/**
	 * constructor with parameter double d
	 * 
	 * @param d
	 */
	public TDNumber(double d) {
	}

	/**
	 * constructor with parameter long l
	 * 
	 * @param l
	 */
	public TDNumber(long l) {
	}

	/**
	 * constructor with parameter string str
	 * 
	 * @param l
	 */
	public TDNumber(String str) {
	}

	/**
	 * default to do <br>
	 * plus 1
	 */
	public abstract void add();

	/**
	 * default to do <br>
	 * minus 1
	 */
	public abstract void minus();

	/**
	 * default to do <br>
	 * multiply 1
	 */
	public abstract void multiply();

	/**
	 * default to do <br>
	 * divide 1
	 */
	public abstract void divide();

	/**
	 * plus parameter t
	 * 
	 * @param t
	 */
	public abstract void add(TDNumber t);

	/**
	 * minus parameter t
	 * 
	 * @param t
	 */
	public abstract void minus(TDNumber t);

	/**
	 * multiply parameter t
	 * 
	 * @param t
	 */
	public abstract void multiply(TDNumber t);

	/**
	 * divide parameter t
	 * 
	 * @param t
	 */
	public abstract void divide(TDNumber t);

	/**
	 * plus parameter i
	 * 
	 * @param i
	 */
	public abstract void add(int i);

	/**
	 * minus parameter i
	 * 
	 * @param i
	 */
	public abstract void minus(int i);

	/**
	 * multiply parameter i
	 * 
	 * @param i
	 */
	public abstract void multiply(int i);

	/**
	 * divide parameter i
	 * 
	 * @param i
	 */
	public abstract void divide(int i);

	/**
	 * plus parameter l
	 * 
	 * @param l
	 */
	public abstract void add(long l);

	/**
	 * minus parameter l
	 * 
	 * @param l
	 */
	public abstract void minus(long l);

	/**
	 * multiply parameter l
	 * 
	 * @param l
	 */
	public abstract void multiply(long l);

	/**
	 * divide parameter l
	 * 
	 * @param l
	 */
	public abstract void divide(long l);

	/**
	 * plus parameter d
	 * 
	 * @param d
	 */
	public abstract void add(double d);

	/**
	 * minus parameter d
	 * 
	 * @param d
	 */
	public abstract void minus(double d);

	/**
	 * multiply parameter d
	 * 
	 * @param d
	 */
	public abstract void multiply(double d);

	/**
	 * divide parameter d
	 * 
	 * @param d
	 */
	public abstract void divide(double d);

	/**
	 * get value of type int
	 * 
	 * @return value
	 */
	public abstract int getInt();

	/**
	 * get value of type double
	 * 
	 * @return value
	 */
	public abstract double getDouble();

	/**
	 * get value of type long
	 * 
	 * @return value
	 */
	public abstract long getLong();

}
