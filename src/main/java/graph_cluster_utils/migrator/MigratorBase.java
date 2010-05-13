package graph_cluster_utils.migrator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import graph_gen_utils.general.Consts;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import p_graph_service.PGraphDatabaseService;

public class MigratorBase extends Migrator {

	private PGraphDatabaseService userTransNeo = null;
	private int migrationPeriod = 0;

	public MigratorBase(PGraphDatabaseService userTransNeo, int migrationPeriod) {
		this.migrationPeriod = migrationPeriod;
		this.userTransNeo = userTransNeo;
	}

	@Override
	public void doMigrateNow(GraphDatabaseService algTransNeo, Object variant) {

		if (isMigrateNow(variant) == false)
			return;

		Transaction algTx = algTransNeo.beginTx();
		Transaction userTx = userTransNeo.beginTx();

		try {
			int bufferSize = 1000;
			HashMap<Byte, ArrayList<Node>> nodesBuffer = new HashMap<Byte, ArrayList<Node>>();
			ArrayList<Node> sameColorNodes = null;
			for (Node algNode : algTransNeo.getAllNodes()) {

				Node userNode = userTransNeo.getNodeById(algNode.getId());
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

	@Override
	protected boolean isMigrateNow(Object variant) {
		long timeStep = (Integer) variant;
		return (timeStep % migrationPeriod) == 0;
	}

}
