package graph_cluster_utils.logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import graph_cluster_utils.config.Conf;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.NeoFromFile;

/**
 * Generic implementation of {@link Logger}.
 * 
 * Logs .gml, .graph, .ptn, and .met files from given
 * {@link GraphDatabaseService}
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class LoggerBase extends Logger {

	private int snapshotPeriod = -1;
	private String graphName = "";
	private String resultsDir = "";

	public LoggerBase(int snapshotPeriod, String graphName, String resultsDir) {
		super();
		this.snapshotPeriod = snapshotPeriod;
		this.graphName = graphName;
		this.resultsDir = resultsDir;
	}

	@Override
	protected boolean isInitialSnapshot() {
		return true;
	}

	@Override
	public void doInitialSnapshot(GraphDatabaseService transNeo, Conf baseConfig) {

		ConfDiDiC config = (ConfDiDiC) baseConfig;

		if (isInitialSnapshot() == false)
			return;

		String outMetrics = String.format("%s%s.%d.INIT.met", resultsDir,
				graphName, config.getClusterCount());

		// Write graph metrics to file
		NeoFromFile.writeMetricsCSV(transNeo, outMetrics);
	}

	@Override
	protected boolean isPeriodicSnapshot(Object variant) {
		int timeStep = (Integer) variant;
		return (timeStep % snapshotPeriod) == 0;
	}

	@Override
	public void doPeriodicSnapshot(GraphDatabaseService transNeo,
			Object variant, Conf baseConfig) {

		ConfDiDiC config = (ConfDiDiC) baseConfig;
		int timeStep = (Integer) variant;

		if (isPeriodicSnapshot(timeStep) == false)
			return;

		String outMetrics = String.format("%s%s.%d.met", resultsDir, graphName,
				config.getClusterCount());

		// Write graph metrics to file
		NeoFromFile.appendMetricsCSV(transNeo, outMetrics, (long) timeStep);

	}

	@Override
	protected boolean isFinalSnapshot() {
		return true;
	}

	@Override
	public void doFinalSnapshot(GraphDatabaseService transNeo, Conf baseConfig) {

		ConfDiDiC config = (ConfDiDiC) baseConfig;

		if (isFinalSnapshot() == false)
			return;

		String outMetrics = String.format("%s%s.%d.FINAL.met", resultsDir,
				graphName, config.getClusterCount());

		// Write graph metrics to file
		NeoFromFile.appendMetricsCSV(transNeo, outMetrics, null);
	}

}
