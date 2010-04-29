package graph_cluster_utils.change_log;

import java.util.HashMap;

public class ChangeOpAddNode extends ChangeOp {

	private long nodeId = 0;
	private byte color = -1;

	public ChangeOpAddNode(long nodeId, byte color,
			HashMap<String, Object> properties) {
		throw new UnsupportedOperationException("Properties not yet supported");
	}

	public ChangeOpAddNode(long nodeId, byte color) {
		this.nodeId = nodeId;
		this.color = color;
	}

	public long getNodeId() {
		return nodeId;
	}

	public byte getColor() {
		return color;
	}

}
