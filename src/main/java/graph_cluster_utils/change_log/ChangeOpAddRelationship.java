package graph_cluster_utils.change_log;

import java.util.HashMap;

public class ChangeOpAddRelationship extends ChangeOp {

	private long startNodeId = 0;
	private long endNodeId = 0;

	public ChangeOpAddRelationship(long startNodeId, long endNodeId,
			HashMap<String, Object> properties) {
		throw new UnsupportedOperationException("Properties not yet supported");
	}

	public ChangeOpAddRelationship(long startNodeId, long endNodeId) {
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
