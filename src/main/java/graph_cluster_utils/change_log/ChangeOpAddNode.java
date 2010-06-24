package graph_cluster_utils.change_log;

import java.util.HashMap;

public class ChangeOpAddNode extends ChangeOp {

	private long id = 0;
	private byte color = -1;

	public ChangeOpAddNode(long id, byte color,
			HashMap<String, Object> properties) {
		throw new UnsupportedOperationException("Properties not yet supported");
	}

	public ChangeOpAddNode(long id, byte color) {
		this.id = id;
		this.color = color;
	}

	public long getNodeId() {
		return id;
	}

	public byte getColor() {
		return color;
	}

	@Override
	public String toString() {
		return String.format("%s ID[%d] Color[%d]", getChangeOpId(), id, color);
	}

}
