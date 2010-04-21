package graph_cluster_utils.change_log;

public class ChangeOpEnd extends ChangeOp {

	@Override
	public String getChangeOpId() {
		return this.getClass().getName();
	}

}
