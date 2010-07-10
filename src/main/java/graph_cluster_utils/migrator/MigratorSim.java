package graph_cluster_utils.migrator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import jobs.SimJob;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.change_log.LogReaderChangeOp;
import graph_gen_utils.general.Consts;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import applications_old.EdgeCutCrawler;

import p_graph_service.PGraphDatabaseService;

public class MigratorSim extends Migrator {

	private PGraphDatabaseService userTransNeo = null;
	private int migrationPeriod = 0;
	private LinkedBlockingQueue<ChangeOp> changeOpLog = null;
	private SimJob simJob = null;
	private String[] changeOpLogFiles = null;
	private int changeOpLogFilesIndex = -1;

	public MigratorSim(PGraphDatabaseService userTransNeo, int migrationPeriod,
			Queue<ChangeOp> changeLog, SimJob simJob, String[] changeOpLogFiles) {
		this.migrationPeriod = migrationPeriod;
		this.userTransNeo = userTransNeo;
		this.changeOpLog = (LinkedBlockingQueue<ChangeOp>) changeLog;
		this.simJob = simJob;
		this.changeOpLogFiles = changeOpLogFiles;
	}

	@Override
	public void doMigrateNow(GraphDatabaseService algTransNeo, Object variant) {

		if (isMigrateNow(variant) == false)
			return;

		// Update UserDB with changes that DiDiC made to AlgDB
		doMigrateNow(algTransNeo);

		// Perform read operations on PDBSim to measure impact of DiDiC & Churn
		try {
			simJob.start();
		} catch (Exception eReadOps) {
			eReadOps.printStackTrace();
		}

		// Calculate Edgecut, Relationship Balance, Node Balance, etc
		String edgecutCrawlerFilePath = String.format("edgecutCrawler_%d.csv",
				System.currentTimeMillis());
		EdgeCutCrawler.calOnDB(userTransNeo, edgecutCrawlerFilePath, 4);

		if (++changeOpLogFilesIndex >= changeOpLogFiles.length)
			return;

		// Read ChangeOpLog into ChangeOpQueue for DiDiC
		String changeOpLogFilePath = changeOpLogFiles[changeOpLogFilesIndex];
		if (changeOpLogFilePath == null)
			// Dummy
			return;

		File changeOpFile = new File(changeOpLogFilePath);
		LogReaderChangeOp logReader = new LogReaderChangeOp(changeOpFile);

		for (ChangeOp changeOp : logReader.getChangeOps()) {
			try {
				changeOpLog.put(changeOp);
			} catch (Exception eChangeOps) {
				eChangeOps.printStackTrace();
			}
		}
	}

	@Override
	protected boolean isMigrateNow(Object variant) {
		long timeStep = (Integer) variant;
		return (timeStep % migrationPeriod) == 0;
	}

	private void doMigrateNow(GraphDatabaseService algTransNeo) {

		Transaction algTx = algTransNeo.beginTx();
		Transaction userTx = userTransNeo.beginTx();

		try {
			// TODO Replace hardcoded value with constant
			int bufferSize = 1000;
			HashMap<Byte, ArrayList<Node>> nodesBuffer = new HashMap<Byte, ArrayList<Node>>();
			ArrayList<Node> sameColorNodes = null;
			for (Node algNode : algTransNeo.getAllNodes()) {

				long nodeId = algNode.getId();
				Node userNode = userTransNeo.getNodeById(nodeId);

				Byte algNodeColor = (Byte) algNode.getProperty(Consts.COLOR);

				sameColorNodes = nodesBuffer.get(algNodeColor);
				if (sameColorNodes == null) {
					sameColorNodes = new ArrayList<Node>();
					nodesBuffer.put(algNodeColor, sameColorNodes);
				}
				sameColorNodes.add(userNode);

				if (sameColorNodes.size() % bufferSize == 0) {
					userTransNeo.moveNodes(sameColorNodes, algNodeColor);

					sameColorNodes.clear();

					userTx.success();
					userTx.finish();
					userTx = userTransNeo.beginTx();
				}

			}

			for (Entry<Byte, ArrayList<Node>> sameColorNodesEntry : nodesBuffer
					.entrySet()) {

				String x = "";
				for (Node node : sameColorNodesEntry.getValue()) {
					x += "," + node.getId();
				}

				if (sameColorNodesEntry.getValue().size() > 0) {
					userTransNeo.moveNodes(sameColorNodesEntry.getValue(),
							sameColorNodesEntry.getKey());

					sameColorNodesEntry.getValue().clear();

					userTx.success();
					userTx.finish();
					userTx = userTransNeo.beginTx();
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			algTx.finish();
			if (userTx != null)
				userTx.finish();
		}
	}

}
