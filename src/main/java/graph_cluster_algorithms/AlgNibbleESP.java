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
		// TODO
	}

	private void init() {
		// TODO
		// Set X0
		// Set S0
	}

	private void evoPartition() {
		// TODO
	}

	private void evoCut(Node v, double conductance) {
		double T = Math.floor(Math.pow(conductance, -1) / 100.0);
		HashMap<Long, Long> S = genSample(v, T, Long.MAX_VALUE);
		// TODO
	}

	private void evoNibble() {
		// TODO
	}

	// Simulates volume-biased Evolving Set Process
	// Updates boundary of current set at each step
	// Generates sample path of sets and outputs last set
	//
	// INPUT
	// ----> x : Starting vertex
	// ----> T>=0 : Time limit
	// ----> B>=0 : Budget
	// OUTPUT
	// ----> St : Set sampled from volume-biased Evolving Set Process
	private HashMap<Long, Long> genSample(Node x, double T, double B) {
		// TODO

		DSNibbleESP sAndB = new DSNibbleESP(); // S, B, volume, conductance
		Node X = null; // Current random-walk position @ time t
		
		// Init
		// ----> X = x0 = x
		X = x;
		// ----> S = S0 = {x}
		sAndB.getS().put(x.getId(), null);
		
		// ForEach Step t <= T
		// -> STAGE 1: compute St-1 to St difference
		// ----> X = Choose X with p(Xt-1,Xt)
		// ----> Compute probYinS(X)
		// ----> Select random threshold Z = getZ(X)
		// ----> St = {y | probYinS(y,St-1) > Z}
		// ----> D = Set different between St & St-1
		// ----> Update volume(St) & cost(S0,...,St)
		// ----> IF t==T OR cost()>B RETURN St = St-1 DIFF D FIXME DIFF==UNION?
		// -> STAGE 2: update S to St by adding/removing vertices in D to S
		// ----> Add/remove vertices in D to S
		// ----> Update B(St-1) to B(St)
		// ----> Compute conductance(St) = B(St) / volume(St)
		// ----> IF conductance(St) < ThetaT RETURN St

		return new HashMap<Long, Long>();
	}

	// Stopping Rule
	private boolean t(double T, double B) {
		// TODO
		return false;
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
