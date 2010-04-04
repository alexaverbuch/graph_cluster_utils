package graph_cluster_utils.supervisor;

import graph_cluster_utils.alg.disk.AlgDisk;
import graph_cluster_utils.alg.mem.AlgMem;

/**
 * Base class of all supervisors.
 * 
 * Supervisors are passed to {@link AlgDisk} and {@link AlgMem} implementations.
 * They're used to delegate logging and dynamism (CRUD) operations to.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class Supervisor {

	/**
	 * Checks if dynamism should performed at this timeStep.
	 * 
	 * @param timeStep
	 *            represents the algorithms current iteration
	 */
	public abstract boolean isDynamism(int timeStep);

	/**
	 * Performed dynamism on given database.
	 * 
	 * @param databaseDir
	 *            path to a Neo4j instance
	 */
	public abstract void doDynamism(String databaseDir);

	/**
	 * Checks if initial logging/snapshot should be performed.
	 */
	public abstract boolean isInitialSnapshot();

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param clusterCount
	 *            number of clusters the algorithm is either trying to find, or
	 *            has found so far (depending on use)
	 * 
	 * @param databaseDir
	 *            path to a Neo4j instance
	 */
	public abstract void doInitialSnapshot(int clusterCount, String databaseDir);

	/**
	 * Checks if logging/snapshot should be performed at this time step.
	 * 
	 * @param timeStep
	 *            represents the algorithms current iteration
	 */
	public abstract boolean isPeriodicSnapshot(long timeStep);

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param timeStep
	 *            represents the algorithms current iteration
	 * 
	 * @param clusterCount
	 *            number of clusters the algorithm is either trying to find, or
	 *            has found so far (depending on use)
	 * 
	 * @param databaseDir
	 *            path to a Neo4j instance
	 */
	public abstract void doPeriodicSnapshot(long timeStep, int clusterCount,
			String databaseDir);

	/**
	 * Checks if final logging/snapshot should be performed.
	 */
	public abstract boolean isFinalSnapshot();

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param clusterCount
	 *            number of clusters the algorithm is either trying to find, or
	 *            has found so far (depending on use)
	 * 
	 * @param databaseDir
	 *            path to a Neo4j instance
	 */
	public abstract void doFinalSnapshot(int clusterCount, String databaseDir);
}
