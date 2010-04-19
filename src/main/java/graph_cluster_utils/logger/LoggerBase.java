package graph_cluster_utils.logger;

import org.neo4j.graphdb.GraphDatabaseService;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;

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
	private int longSnapshotPeriod = -1;
	private String graphName = "";
	private String resultsDir = "";

	public LoggerBase(int snapshotPeriod, int longSnapshotPeriod,
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
	public void doInitialSnapshot(GraphDatabaseService transNeo,
			int clusterCount) {

		if (isInitialSnapshot() == false)
			return;

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, clusterCount);

			String outGraph = String
					.format("%s%s.graph", resultsDir, graphName);

			String outPtn = String.format("%s%s.%d.ptn", resultsDir, graphName,
					clusterCount);

			// Write graph metrics to file
			NeoFromFile.writeMetricsCSV(transNeo, outMetrics);

			// Write chaco file and initial partitioning to file
			// .ptn file can be used in future simulations for consistency
			NeoFromFile.writeChacoAndPtn(transNeo, outGraph,
					ChacoType.UNWEIGHTED, outPtn);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	protected boolean isPeriodicSnapshot(long timeStep) {
		return (timeStep % snapshotPeriod) == 0;
	}

	@Override
	public void doPeriodicSnapshot(GraphDatabaseService transNeo,
			long timeStep, int clusterCount) {

		if (isPeriodicSnapshot(timeStep) == false)
			return;

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, clusterCount);

			// Write graph metrics to file
			NeoFromFile.appendMetricsCSV(transNeo, outMetrics, timeStep);

			String outGml = String.format("%s%s.%d.%d.gml", resultsDir,
					graphName, clusterCount, timeStep);

			if ((timeStep % longSnapshotPeriod) == 0)
				NeoFromFile.writeGMLBasic(transNeo, outGml);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	protected boolean isFinalSnapshot() {
		return true;
	}

	@Override
	public void doFinalSnapshot(GraphDatabaseService transNeo, int clusterCount) {

		if (isFinalSnapshot() == false)
			return;

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, clusterCount);

			// Write graph metrics to file
			NeoFromFile.appendMetricsCSV(transNeo, outMetrics, null);

			String outGml = String.format("%s%s.%d.gml", resultsDir, graphName,
					clusterCount);

			NeoFromFile.writeGMLBasic(transNeo, outGml);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
