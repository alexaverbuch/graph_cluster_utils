package graph_cluster_utils.migrator;

import java.util.ArrayList;

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

		try {
			for (Node algNode : algTransNeo.getAllNodes()) {

				Transaction userTx = userTransNeo.beginTx();

				try {

					Long algNodeId = (Long) algNode
							.getProperty(Consts.NODE_GID);
					Node userNode = userTransNeo.getNodeById(algNodeId);

					ArrayList<Node> nodes = new ArrayList<Node>();
					nodes.add(userNode);

					userTransNeo.moveNodes(nodes, (Long) algNode
							.getProperty(Consts.COLOR));

					userTx.success();

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					userTx.finish();
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			algTx.finish();
		}
	}

	@Override
	protected boolean isMigrateNow(Object variant) {
		long timeStep = (Long) variant;
		return (timeStep % migrationPeriod) == 0;
	}

}
