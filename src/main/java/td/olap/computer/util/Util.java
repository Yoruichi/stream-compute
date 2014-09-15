package td.olap.computer.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

	private static Logger logger = LoggerFactory.getLogger(Util.class);

	/**
	 * <p>
	 * 对象与字节数组之间的转换工具类,要求传入的对象必须实现序列号接口.<br>
	 * Between the object and byte array conversion tool category, requiring the
	 * incoming object must implement the interface serial number.
	 * </p>
	 * 
	 * @param obj
	 *            the obj
	 * @return the byte[]
	 */

	/**
	 * 对象转换成字节数组,要求传入的对象必须实现序列号接口.<br>
	 * Object into a byte array, requiring incoming object must implement the
	 * interface to the serial number.
	 * 
	 * @param obj
	 * @return byte[]
	 */
	public static byte[] ObjectToByte(Serializable obj) {
		byte[] bytes = null;
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);

			bytes = bo.toByteArray();

			bo.close();
			oo.close();
		} catch (Exception e) {
			logger.error(obj.toString(), e);
			e.printStackTrace();
		}
		return bytes;
	}

	/**
	 * 字节数组转换成对象.<br>
	 * Byte array into an object.
	 * 
	 * @param bytes
	 *            the bytes
	 * @return Object 取得结果后强制转换成你存入的对象类型
	 */
	public static Serializable ByteToObject(byte[] bytes) {
		if (bytes == null)
			return null;
		java.lang.Object obj = null;
		try {
			ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
			ObjectInputStream oi = new ObjectInputStream(bi);

			obj = oi.readObject();

			bi.close();
			oi.close();
		} catch (Exception e) {
			logger.error(Arrays.toString(bytes), e);
			e.printStackTrace();
		}
		return (Serializable) obj;
	}

}
