package graph_cluster_utils.alg.disk;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import graph_cluster_utils.alg.Alg;
import graph_cluster_utils.supervisor.Supervisor;

/**
 * Base class for all clustering/partitioning algorithms that compute directly
 * on a Neo4j instance, rather than an in-memory graph.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class AlgDisk implements Alg {

	protected String databaseDir = null;
	protected Supervisor supervisor = null;
	protected GraphDatabaseService transNeo = null;
	protected IndexService transIndexService = null;

	public AlgDisk(String databaseDir, Supervisor supervisor) {
		this.databaseDir = databaseDir;
		this.supervisor = supervisor;
	}

	protected void openTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Transactional Services...");

		transNeo = new EmbeddedGraphDatabase(databaseDir);
		transIndexService = new LuceneIndexService(transNeo);

		// PRINTOUT
		System.out
				.printf("%s%n", getTimeStr(System.currentTimeMillis() - time));
	}

	protected void closeTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Closing Transactional Services...");

		transIndexService.shutdown();
		transNeo.shutdown();

		// PRINTOUT
		System.out
				.printf("%s%n", getTimeStr(System.currentTimeMillis() - time));
	}

	protected String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

}
