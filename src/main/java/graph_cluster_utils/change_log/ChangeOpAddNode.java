package graph_cluster_utils.change_log;

import java.util.HashMap;

public class ChangeOpAddNode extends ChangeOp {

	private long nodeId = 0;

	public ChangeOpAddNode(long nodeId, HashMap<String, Object> properties) {
		throw new UnsupportedOperationException("Properties not yet supported");
	}

	public ChangeOpAddNode(long nodeId) {
		this.nodeId = nodeId;
	}

	public long getNodeId() {
		return nodeId;
	}

}
