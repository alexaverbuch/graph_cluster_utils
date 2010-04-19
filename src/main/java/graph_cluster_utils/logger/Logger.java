package graph_cluster_utils.logger;

import graph_cluster_utils.ptn_alg.PtnAlg;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Base class of all supervisors.
 * 
 * Loggers are passed to {@link PtnAlg} implementations. They're used to
 * delegate logging operations to.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class Logger {

	/**
	 * Checks if initial logging/snapshot should be performed.
	 */
	protected abstract boolean isInitialSnapshot();

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} implementation to be read from
	 *            during logging
	 * 
	 * @param clusterCount
	 *            number of clusters the algorithm is either trying to find, or
	 *            has found so far (depending on use)
	 * 
	 */
	public abstract void doInitialSnapshot(GraphDatabaseService transNeo,
			int clusterCount);

	/**
	 * Checks if logging/snapshot should be performed at this time step.
	 * 
	 * @param timeStep
	 *            represents the algorithms current iteration
	 */
	protected abstract boolean isPeriodicSnapshot(long timeStep);

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} implementation to be read from
	 *            during logging
	 * 
	 * @param timeStep
	 *            represents the algorithms current iteration
	 * 
	 * @param clusterCount
	 *            number of clusters the algorithm is either trying to find, or
	 *            has found so far (depending on use)
	 * 
	 */
	public abstract void doPeriodicSnapshot(GraphDatabaseService transNeo,
			long timeStep, int clusterCount);

	/**
	 * Checks if final logging/snapshot should be performed.
	 */
	protected abstract boolean isFinalSnapshot();

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} implementation to be read from
	 *            during logging
	 * 
	 * @param clusterCount
	 *            number of clusters the algorithm is either trying to find, or
	 *            has found so far (depending on use)
	 * 
	 */
	public abstract void doFinalSnapshot(GraphDatabaseService transNeo,
			int clusterCount);
}
