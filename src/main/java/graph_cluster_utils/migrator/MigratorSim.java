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

import applications.GISGenerateOperations;

import p_graph_service.PGraphDatabaseService;

public class MigratorSim extends Migrator {

	private PGraphDatabaseService userTransNeo = null;
	private int migrationPeriod = 0;
	private LinkedBlockingQueue<ChangeOp> changeOpLog = null;
	private SimJob simJob = null;

	public MigratorSim(PGraphDatabaseService userTransNeo, int migrationPeriod,
			Queue<ChangeOp> changeLog, SimJob simJob) {
		this.migrationPeriod = migrationPeriod;
		this.userTransNeo = userTransNeo;
		this.changeOpLog = (LinkedBlockingQueue<ChangeOp>) changeLog;
		this.simJob = simJob;
	}

	@Override
	public void doMigrateNow(GraphDatabaseService algTransNeo, Object variant) {

		if (isMigrateNow(variant) == false)
			return;

		// Update UserDB with changes that DiDiC made to AlgDB
		doMigrateNow(algTransNeo);

		// Create new ChangeOpLog location for PDBSim to store churn in
		String prevChangeOpFileStr = ((PGraphDatabaseService) userTransNeo)
				.getDBChangeLog();
		int slashIndex = (prevChangeOpFileStr.lastIndexOf("/") == -1) ? 0
				: prevChangeOpFileStr.lastIndexOf("/");
		String prevChangeOpDirStr = prevChangeOpFileStr
				.substring(0, slashIndex);
		String churnChangeOpFileStr = String.format("%s/change_op_log_%d.txt",
				prevChangeOpDirStr, System.currentTimeMillis());
		((PGraphDatabaseService) userTransNeo)
				.setDBChangeLog(churnChangeOpFileStr);

		// Perform churn operations on PDBSim
		try {
			simJob.start();
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		// Read new ChangeOpLog into ChangeOpQueue for DiDiC
		File churnChangeOpFile = new File(churnChangeOpFileStr);
		LogReaderChangeOp logReader = new LogReaderChangeOp(churnChangeOpFile);

		for (ChangeOp changeOp : logReader.getChangeOps()) {
			try {
				changeOpLog.put(changeOp);
			} catch (Exception e) {
				e.printStackTrace();
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

				// NOTE
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
				System.out.println(x);

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
