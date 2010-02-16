package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import graph_cluster_supervisor.Supervisor;

public class AlgNibbleESP {

	// ESP Related

	private String databaseDir;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public void start(String databaseDir, ConfNibbleESP config,
			Supervisor supervisor) {
		this.databaseDir = databaseDir;
	}

	private void evoPartition() {

	}

	private void evoCut(Node v, double conductance) {
		double T = Math.floor(Math.pow(conductance, -1) / 100.0);
		HashMap<Long, Long> S = genSample(v, T, Long.MAX_VALUE);
	}

	private void evoNibble() {

	}

	// Simulates volume-biased Evolving Set Process
	// Updates boundary of current set at each step
	// Generates sample path of sets and outputs last set
	//
	// x = Starting vertex, T = Time limit, B = Budget
	private HashMap<Long, Long> genSample(Node x, double T, double B) {
		return new HashMap<Long, Long>();
	}

	private void openTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Opening Transactional Services...");

		transNeo = new EmbeddedGraphDatabase(this.databaseDir);
		transIndexService = new LuceneIndexService(transNeo);

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void closeTransServices() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Closing Transactional Services...");

		transIndexService.shutdown();
		transNeo.shutdown();

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

}
