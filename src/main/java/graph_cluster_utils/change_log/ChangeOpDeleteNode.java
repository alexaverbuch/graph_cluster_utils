package graph_cluster_utils.change_log;

public class ChangeOpDeleteNode extends ChangeOp {

	private long nodeId = 0;

	public ChangeOpDeleteNode(long nodeId) {
		this.nodeId = nodeId;
	}

	public long getNodeId() {
		return nodeId;
	}

}
