package graph_cluster_utils.change_log;

public class ChangeOpDeleteNode extends ChangeOp {

	private long id = 0;

	public ChangeOpDeleteNode(long id) {
		this.id = id;
	}

	public long getNodeId() {
		return id;
	}

	@Override
	public String toString() {
		return String.format("%s ID[%d]", getChangeOpId(), id);
	}

}
