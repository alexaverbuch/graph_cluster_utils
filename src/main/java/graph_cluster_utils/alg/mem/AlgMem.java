package graph_cluster_utils.alg.mem;

import graph_cluster_utils.alg.Alg;
import graph_cluster_utils.supervisor.Supervisor;
import graph_gen_utils.memory_graph.MemGraph;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Base class for all clustering/partitioning algorithms that compute on an
 * in-memory graph, rather than directly on a Neo4j instance.
 * 
 * Changes are flushed from memory into a Neo4j instance.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class AlgMem implements Alg {

	protected String databaseDir = null;
	protected Supervisor supervisor = null;
	protected GraphDatabaseService transNeo = null;
	protected IndexService transIndexService = null;
	protected MemGraph memGraph = null;

	public AlgMem(String databaseDir, Supervisor supervisor, MemGraph memGraph) {
		this.databaseDir = databaseDir;
		this.supervisor = supervisor;
		this.memGraph = memGraph;
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
