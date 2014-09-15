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

	public Tuple getMessage(String field) {
		if (messages == null)
			return null;
		return messages.get(field);
	}

	public void setMessage(Serializable... message) {
		this.message = message;
	}

	public Map<String, Tuple> getMessages() {
		return messages;
	}

	public void setMessages(Map<String, Tuple> messages) {
		this.messages = messages;
	}

	private long xid;
	private Serializable[] message;
	private Map<String, Tuple> messages = new HashMap<String, Tuple>();

	public EmitItem() {
	}

	public EmitItem(long xid, Serializable[] message) {
		this.message = message;
		this.xid = xid;
	}

	public EmitItem(long xid, String field, Serializable[] messages) {
		this.xid = xid;
		this.messages.put(field, new Tuple(field, messages));
	}

}
