package graph_cluster_utils.change_log;

public class ChangeOpDeleteRelationship extends ChangeOp {

	private long startNodeId = 0;
	private long endNodeId = 0;

	public ChangeOpDeleteRelationship(long startNodeId, long endNodeId) {
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
	}

	public long getStartNodeId() {
		return startNodeId;
	}

	public long getEndNodeId() {
		return endNodeId;
	}

}
