package graph_cluster_utils.migrator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.change_log.LogReaderChangeOp;
import graph_gen_utils.general.Consts;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import p_graph_service.PGraphDatabaseService;
import sim_tst.GISGenerateOperations;

public class MigratorGISSim extends Migrator {

	private PGraphDatabaseService userTransNeo = null;
	private int migrationPeriod = 0;
	private LinkedBlockingQueue<ChangeOp> changeOpLog = null;
	private File operationLogDir = null;

	public MigratorGISSim(PGraphDatabaseService userTransNeo,
			int migrationPeriod, Queue<ChangeOp> changeLog,
			String operationLogDir) {
		this.migrationPeriod = migrationPeriod;
		this.userTransNeo = userTransNeo;
		this.changeOpLog = (LinkedBlockingQueue<ChangeOp>) changeLog;
		this.operationLogDir = new File(operationLogDir);
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
		String operationLogName = String.format("%s/operation_log_%d.csv",
				operationLogDir.getAbsolutePath(), System.currentTimeMillis());
		GISGenerateOperations.start(operationLogName, userTransNeo, 0.5d, 0.5d,
				0d, 0d, 0d, 5l);

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

				// FIXME Test this
				// GID can be set. userNode & algNode must have matching IDs
				long nodeId = algNode.getId();
				// long nodeId = (Long) algNode.getProperty(Consts.NODE_GID);
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
