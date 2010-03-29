package graph_cluster_algorithms.supervisors;

import java.io.File;

import pGraphService.PGraphDatabaseService;
import pGraphService.PGraphDatabaseServiceImpl;

import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;
import graph_gen_utils.chaco.ChacoWriterUnweighted;
import graph_gen_utils.gml.GMLWriterUndirected;
import graph_gen_utils.metrics.MetricsWriterUnweighted;

public class SupervisorPara extends Supervisor {

	private int snapshotPeriod = -1;
	private int longSnapshotPeriod = -1;
	private String graphName = "";
	private String resultsDir = "";

	public SupervisorPara(int snapshotPeriod, int longSnapshotPeriod,
			String graphName, String graphDir, String ptnDir, String resultsDir) {
		super();
		this.snapshotPeriod = snapshotPeriod;
		this.longSnapshotPeriod = longSnapshotPeriod;
		this.graphName = graphName;
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

			String outGml = String.format("%s%s.%d.-.gml", resultsDir,
					graphName, clusterCount);

			PGraphDatabaseService transNeo = new PGraphDatabaseServiceImpl(
					databaseDir, 1);

			File metricsFile = new File(outMetrics);
			MetricsWriterUnweighted
					.writeMetricsCSV(transNeo, metricsFile, null);

			File chacoFile = new File(outGraph);
			File ptnFile = new File(outPtn);
			ChacoWriterUnweighted chacoWriter = new ChacoWriterUnweighted();
			chacoWriter.write(transNeo, chacoFile, ptnFile);

			File gmlFile = new File(outGml);
			GMLWriterUndirected gmlWriter = new GMLWriterUndirected();
			gmlWriter.write(transNeo, gmlFile);

			transNeo.shutdown();

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

			String outGml = String.format("%s%s.%d.%d.gml", resultsDir,
					graphName, clusterCount, timeStep);

			PGraphDatabaseService transNeo = new PGraphDatabaseServiceImpl(
					databaseDir, 1);

			File metricsFile = new File(outMetrics);
			MetricsWriterUnweighted.appendMetricsCSV(transNeo, metricsFile,
					timeStep);

			if ((timeStep % longSnapshotPeriod) == 0) {
				File gmlFile = new File(outGml);
				GMLWriterUndirected gmlWriter = new GMLWriterUndirected();
				gmlWriter.write(transNeo, gmlFile);
			}

			transNeo.shutdown();

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

			String outGml = String.format("%s%s.%d.gml", resultsDir, graphName,
					clusterCount);

			PGraphDatabaseService transNeo = new PGraphDatabaseServiceImpl(
					databaseDir, 1);

			File metricsFile = new File(outMetrics);
			MetricsWriterUnweighted.appendMetricsCSV(transNeo, metricsFile,
					null);

			File gmlFile = new File(outGml);
			GMLWriterUndirected gmlWriter = new GMLWriterUndirected();
			gmlWriter.write(transNeo, gmlFile);

			transNeo.shutdown();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
