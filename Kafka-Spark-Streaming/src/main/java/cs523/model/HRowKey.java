package cs523.model;

public abstract class HRowKey {
	private byte[] rowKey;

	public HRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public byte[] getRowKey() {
		return rowKey;
	}
}
