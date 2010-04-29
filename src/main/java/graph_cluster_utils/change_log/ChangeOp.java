package graph_cluster_utils.change_log;

public abstract class ChangeOp {

	public final String getChangeOpId() {
		return this.getClass().getName();
	}

}
