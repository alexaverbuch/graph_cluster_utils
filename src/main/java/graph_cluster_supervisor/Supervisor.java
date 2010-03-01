package graph_cluster_supervisor;

public abstract class Supervisor {
	public abstract boolean isDynamism(int timeStep);

	public abstract void doDynamism(String databaseDir); 

	public abstract boolean isInitialSnapshot();

	public abstract void doInitialSnapshot(int clusterCount,
			String databaseDir);

	public abstract boolean isPeriodicSnapshot(long timeStep);

	public abstract void doPeriodicSnapshot(long timeStep, int clusterCount,
			String databaseDir);

	public abstract boolean isFinalSnapshot();

	public abstract void doFinalSnapshot(int clusterCount, String databaseDir);
}
