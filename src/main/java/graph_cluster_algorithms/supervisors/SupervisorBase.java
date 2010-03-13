package graph_cluster_algorithms.supervisors;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;

public class SupervisorBase extends Supervisor {

	private int snapshotPeriod = -1;
	private int longSnapshotPeriod = -1;
	private String graphName = "";
	private String graphDir = "";
	private String ptnDir = "";
	private String resultsDir = "";

	public SupervisorBase(int snapshotPeriod, int longSnapshotPeriod,
			String graphName, String graphDir, String ptnDir, String resultsDir) {
		super();
		this.snapshotPeriod = snapshotPeriod;
		this.longSnapshotPeriod = longSnapshotPeriod;
		this.graphName = graphName;
		this.graphDir = graphDir;
		this.ptnDir = ptnDir;
		this.resultsDir = resultsDir;
	}

	@Override
	public boolean isDynamism(int timeStep) {
		return false;
	}

	@Override
	public void doDynamism(String databaseDir) {
	}

	@Override
	public boolean isInitialSnapshot() {
		return true;
	}

	@Override
	public void doInitialSnapshot(int clusterCount, String databaseDir) {

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, clusterCount);

			String outGraph = String
					.format("%s%s.graph", resultsDir, graphName);

			String outPtn = String.format("%s%s.%d.ptn", resultsDir, graphName,
					clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(databaseDir);

			// Write graph metrics to file
			neoCreator.writeMetricsCSV(outMetrics);

			// Write chaco file and initial partitioning to file
			// .ptn file can be used in future simulations for consistency
			neoCreator.writeChacoAndPtn(outGraph, ChacoType.UNWEIGHTED, outPtn);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean isPeriodicSnapshot(long timeStep) {
		return (timeStep % snapshotPeriod) == 0;
	}

	@Override
	public void doPeriodicSnapshot(long timeStep, int clusterCount,
			String databaseDir) {

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(databaseDir);

			// Write graph metrics to file
			neoCreator.appendMetricsCSV(outMetrics, timeStep);

			String outGml = String.format("%s%s.%d.%d.gml", resultsDir,
					graphName, clusterCount, timeStep);

			if ((timeStep % longSnapshotPeriod) == 0)
				neoCreator.writeGML(outGml);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean isFinalSnapshot() {
		return true;
	}

	@Override
	public void doFinalSnapshot(int clusterCount, String databaseDir) {

		try {

			String outMetrics = String.format("%s%s.%d.met", resultsDir,
					graphName, clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(databaseDir);

			// Write graph metrics to file
			neoCreator.appendMetricsCSV(outMetrics, null);

			String outGml = String.format("%s%s.%d.gml", resultsDir, graphName,
					clusterCount);

			neoCreator.writeGML(outGml);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
