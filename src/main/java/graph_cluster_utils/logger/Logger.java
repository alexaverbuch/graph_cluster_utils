package graph_cluster_utils.logger;

import graph_cluster_utils.config.Conf;
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
	 * @param config
	 *            {@link Conf} instance used to configure the {@link PtnAlg}
	 *            algorithm
	 * 
	 */
	public abstract void doInitialSnapshot(GraphDatabaseService transNeo,
			Conf config);

	/**
	 * Checks if logging/snapshot should be performed at this time step.
	 * 
	 * @param variant
	 *            state used to decide if a snapshot should be performed
	 * 
	 */
	protected abstract boolean isPeriodicSnapshot(Object variant);

	/**
	 * Perform logging/snapshot of the state of the current Neo4j instance. This
	 * may include, but is not limited to, graph and clustering metrics.
	 * 
	 * @param transNeo
	 *            {@link GraphDatabaseService} implementation to be read from
	 *            during logging
	 * 
	 * @param variant
	 *            state used to decide if a snapshot should be performed
	 * 
	 * @param config
	 *            {@link Conf} instance used to configure the {@link PtnAlg}
	 *            algorithm
	 * 
	 */
	public abstract void doPeriodicSnapshot(GraphDatabaseService transNeo,
			Object variant, Conf config);

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
	 * @param config
	 *            {@link Conf} instance used to configure the {@link PtnAlg}
	 *            algorithm
	 * 
	 */
	public abstract void doFinalSnapshot(GraphDatabaseService transNeo,
			Conf config);
}
