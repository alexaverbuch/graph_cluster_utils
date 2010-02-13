package graph_cluster_utils;

public abstract class Supervisor {
	public abstract boolean is_dynamism(int timeStep);

	public abstract void do_dynamism(String databaseDir); 

	public abstract boolean is_initial_snapshot();

	public abstract void do_initial_snapshot(int clusterCount,
			String databaseDir);

	public abstract boolean is_periodic_snapshot(int timeStep);

	public abstract void do_periodic_snapshot(int timeStep, int clusterCount,
			String databaseDir);

	public abstract boolean is_final_snapshot();

	public abstract void do_final_snapshot(int clusterCount, String databaseDir);
}
