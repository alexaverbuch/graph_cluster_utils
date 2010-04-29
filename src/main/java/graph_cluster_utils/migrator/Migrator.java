package graph_cluster_utils.migrator;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class Migrator {

	public abstract void doMigrateNow(GraphDatabaseService algTransNeo,
			Object variant);

	protected abstract boolean isMigrateNow(Object variant);

}
