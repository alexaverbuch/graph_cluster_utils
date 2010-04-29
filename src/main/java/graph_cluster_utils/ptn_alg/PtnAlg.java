package graph_cluster_utils.ptn_alg;

import java.util.Queue;

import org.neo4j.graphdb.GraphDatabaseService;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.config.Conf;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.migrator.Migrator;

/**
 * Base class for all clustering/partitioning algorithms.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class PtnAlg {

	protected GraphDatabaseService transNeo = null;
	protected Logger logger = null;
	protected Migrator migrator = null;
	protected Queue<ChangeOp> changeLog = null;

	public PtnAlg(GraphDatabaseService transNeo, Logger logger,
			Queue<ChangeOp> changeLog, Migrator migrator) {
		this.transNeo = transNeo;
		this.logger = logger;
		this.changeLog = changeLog;
		this.migrator = migrator;
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
	protected abstract void applyChangeLog(int maxChanges, int maxTimeouts);

	protected String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}
