package graph_cluster_utils.logger;

import org.neo4j.graphdb.GraphDatabaseService;

import graph_cluster_utils.config.Conf;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.ChacoType;

/**
 * Generic implementation of {@link Logger}.
 * 
 * Logs .gml, .graph, .ptn, and .met files from given
 * {@link GraphDatabaseService}
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class LoggerFull extends Logger {

	private int snapshotPeriod = -1;
	private int longSnapshotPeriod = -1;
	private String graphName = "";
	private String resultsDir = "";

	public LoggerFull(int snapshotPeriod, int longSnapshotPeriod,
			String graphName, String resultsDir) {
		super();
		this.snapshotPeriod = snapshotPeriod;
		this.longSnapshotPeriod = longSnapshotPeriod;
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

		String outMetrics = String.format("%s%s.%d.met", resultsDir, graphName,
				config.getClusterCount());

		String outGraph = String.format("%s%s.graph", resultsDir, graphName);

		String outPtn = String.format("%s%s.%d.ptn", resultsDir, graphName,
				config.getClusterCount());

		String outGml = String.format("%s%s.INIT.%d.gml", resultsDir,
				graphName, config.getClusterCount());

		// Write graph metrics to file
		NeoFromFile.writeMetricsCSV(transNeo, outMetrics);

		// Write chaco file and initial partitioning to file
		// .ptn file can be used in future simulations for consistency
		NeoFromFile.writeChacoAndPtn(transNeo, outGraph, ChacoType.UNWEIGHTED,
				outPtn);

		// Write GML to file
		NeoFromFile.writeGMLBasic(transNeo, outGml);

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

		String outGml = String.format("%s%s.%d.%d.gml", resultsDir, graphName,
				config.getClusterCount(), timeStep);

		if ((timeStep % longSnapshotPeriod) == 0)
			NeoFromFile.writeGMLBasic(transNeo, outGml);

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

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, config.getClusterCount());

			// Write graph metrics to file
			NeoFromFile.appendMetricsCSV(transNeo, outMetrics, null);

			String outGml = String.format("%s%s.%d.FINAL.gml", resultsDir,
					graphName, config.getClusterCount());

			NeoFromFile.writeGMLBasic(transNeo, outGml);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
