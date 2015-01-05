package td.olap.computer.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EmitItem implements Serializable {
	private static final long serialVersionUID = -112865592978550323L;

	public long getXid() {
		return xid;
	}

	public void setXid(long xid) {
		this.xid = xid;
	}

	public Serializable[] getMessage() {
		return message;
	}

	public Serializable getMessage(int index) {
		if (message == null)
			return null;
		return message[index];
	}

	public void setMessage(Serializable... message) {
		this.message = message;
	}

	private long xid;
	private Serializable[] message;

	public EmitItem() {
	}

	public EmitItem(long xid, Serializable[] message) {
		this.message = message;
		this.xid = xid;
	}

}
