package graph_cluster_utils.migrator;

import org.neo4j.graphdb.GraphDatabaseService;

public class MigratorDummy extends Migrator {

	@Override
	public void doMigrateNow(GraphDatabaseService algTransNeo, Object variant) {
	}

	@Override
	protected boolean isMigrateNow(Object variant) {
		return false;
	}

}
