package graph_cluster_utils.alg;

import org.neo4j.graphdb.GraphDatabaseService;

import graph_cluster_utils.alg.config.Conf;
import graph_cluster_utils.logger.Logger;

/**
 * Base class for all clustering/partitioning algorithms.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class Alg {

	protected Logger logger = null;
	protected GraphDatabaseService transNeo = null;

	public Alg(GraphDatabaseService transNeo, Logger logger) {
		this.transNeo = transNeo;
		this.logger = logger;
	}

	/**
	 * Run the clustering/partitioning algorithm
	 * 
	 * @param config
	 *            an implementation of {@link Conf} containing algorithm
	 *            specific configuration parameters
	 */
	public abstract void start(Conf config);

	protected String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}
