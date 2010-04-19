package graph_cluster_utils.ptn_alg;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.GraphDatabaseService;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.ptn_alg.config.Conf;

/**
 * Base class for all clustering/partitioning algorithms.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class PtnAlg {

	protected Logger logger = null;
	protected GraphDatabaseService transNeo = null;
	protected Queue<ChangeOp> changeLog = null;

	public PtnAlg(GraphDatabaseService transNeo, Logger logger,
			Queue<ChangeOp> changeLog) {
		this.transNeo = transNeo;
		this.logger = logger;
		this.changeLog = changeLog;
	}

	/**
	 * Run the clustering/partitioning algorithm
	 * 
	 * @param config
	 *            an implementation of {@link Conf} containing algorithm
	 *            specific configuration parameters
	 */
	public abstract void doPartition(Conf config);

	/**
	 * Apply change log (CRUD) operations to the {@link GraphDatabaseService}
	 * graph
	 */
	protected abstract void applyChangeLog();

	protected String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}
