package graph_cluster_utils.change_log;

import java.util.HashMap;

public class ChangeOpAddRelationship extends ChangeOp {

	private long id = -1;
	private long startNodeId = 0;
	private long endNodeId = 0;

	public ChangeOpAddRelationship(long id, long startNodeId, long endNodeId,
			HashMap<String, Object> properties) {
		throw new UnsupportedOperationException("Properties not yet supported");
	}

	public ChangeOpAddRelationship(long id, long startNodeId, long endNodeId) {
		this.id = id;
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
	}

	public long getId() {
		return id;
	}

	public long getStartNodeId() {
		return startNodeId;
	}

	public long getEndNodeId() {
		return endNodeId;
	}

	@Override
	public String toString() {
		return String.format("%s ID[%d] StartID[%d] EndID[%d]",
				getChangeOpId(), id, startNodeId, endNodeId);
	}

}
