package graph_cluster_utils.change_log;

public class ChangeOpDeleteRelationship extends ChangeOp {

	private long id = -1;

	public ChangeOpDeleteRelationship(long id) {
		this.id = id;
	}

	public long getId() {
		return id;
	}

	@Override
	public String toString() {
		return String.format("%s ID[%d]", getChangeOpId(), id);
	}

}
