package graph_cluster_algorithms;

import java.util.HashMap;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import org.uncommons.maths.random.ExponentialGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

import graph_cluster_supervisor.Supervisor;

public class AlgNibbleESP {

	// ESP Related
	// private Double thetaT = new Double(0);

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

	// theta
	private void evoPartition(Double theta, Double p) {
		// TODO

		// Set W(j) = W(0) = V
		// Set j = 0
		// Set conductance = theta/7

		// [WHILE] j < 12.m.Ceil( lg(1/p) ) [AND] volumeWj >= (3/4)volumeV
		// -> Set j = j+1
		// -> D(j) = evoNibble(G[W(j-1)], conductance)
		// -> W(j) = W(j-1) - D(j)

		// Set D = D(1) U ... U D(j)
	}

	// // To get balanced cuts, evoCut is replaced by evoNibble
	// // NOTE Not used. Only added for reference & consistency with paper
	// private void evoCut(Node v, double conductance) {
	//
	// Double T = Math.floor(Math.pow(conductance, -1) / 100.0);
	// Double thetaT = new Double(0);
	// DSNibbleESP sAndB = genSample(v, T, Double.MAX_VALUE, thetaT);
	//
	// }

	private HashMap<Long, Long> evoNibble(Double conductance) {
		Long volumeG = getVolumeG(-1);

		// T = Floor(conductance^-1 / 100)
		Double T = Math.floor(Math.pow(conductance, -1) / 100.0);

		// thetaT = Sqrt(4.T^-1.log_2(volume(G)) )
		Double log_2_volumeG = Math.log(volumeG) / Math.log(2);
		Double thetaT = Math.sqrt(4.0 * Math.pow(T, -1) * log_2_volumeG);

		// TODO
		// Choose random vertex with probability P(X=x) = d(x)/volume(G)
		Node v = getRandomNode();

		// Choose random budget
		// -> Let jMax = Ceil(log_2(volumeG))
		Double jMax = Math.ceil(log_2_volumeG);
		// -> Let j be an integer in the range [0,jMax]
		// -> Choose budget with probability P(J=j) = constB.2^-j
		// ----> Where constB is a proportionality constant
		Double constB = new Double(1);
		Random rng = new MersenneTwisterRNG();
		// NOTE exponential & mean = 1 is similar to constB.2^-j & constB = 1
		ExponentialGenerator gen = new ExponentialGenerator(constB, rng);
		Double j = gen.nextValue() * jMax;
		// -> Let Bj = 8.y.2^j
		// ----> Where y = 1 + 4.Sqrt(T.log_2(volumeG))
		Double y = 1 + 4 * Math.sqrt(T * log_2_volumeG);
		Double Bj = 8 * y * Math.pow(2, j);

		DSNibbleESP sAndB = genSample(v, T, Bj, thetaT);

		if ((sAndB.getConductance() > 3 * thetaT)
				&& (sAndB.getVolume() < (3 / 4) * volumeG))
			return sAndB.getS();

		return null;
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
	private DSNibbleESP genSample(Node x, Double T, Double B, Double thetaT) {

		DSNibbleESP sAndB = null; // S, B, volume, conductance
		Node X = null; // Current random-walk position @ time t
		Double Z = new Double(0);
		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>();
		Double conductance = new Double(0);

		// Init
		// -> X = x0 = x
		X = x;
		// -> S = S0 = {x}
		sAndB = new DSNibbleESP(X);

		// ForEach Step t <= T
		for (int t = 0; t < T; t++) {
			// STAGE 1: compute St-1 to St difference
			// -> X = Choose X with p(Xt-1,Xt)
			X = sAndB.getNextV(X);
			// -> Compute probYinS(X)
			// -> Select random threshold Z = getZ(X)
			Z = sAndB.getZ(X);
			// -> Define St = {y | probYinS(y,St-1) > Z}
			// -> D = Set different between St & St-1
			// -> Update volume(St) & cost(S0,...,St)
			D = sAndB.computeDVolumeCost(Z, transNeo);
			// -> IF t==T OR cost()>B RETURN St = St-1 Diff D
			if (sAndB.getCost() > B) {
				// Add/remove vertices in D to S
				sAndB.applyDToS(D);
				break;
			}
			// STAGE 2: update S to St by adding/removing vertices in D to S
			// -> Add/remove vertices in D to S
			// -> Update B(St-1) to B(St)
			sAndB.applyDToS(D);
			// -> Compute conductance(St) = B(St) / volume(St)
			// -> IF conductance(St) < thetaT RETURN St
			if (sAndB.getConductance() < thetaT)
				break;
		}

		return sAndB;
	}

	// Choose random vertex from remaining (unpartitioned) vertices
	// Choose vertex with probability P(X=x) = d(x)/volume(G)
	private Node getRandomNode() {
		// TODO
		return null;
	}

	// Only count nodes with "color" == color
	private Long getVolumeG(Integer color) {
		// TODO only count nodes with "color" == color
		// TODO dont count external edges (e.g. to other colors)
		Long volumeG = new Long(0);

		for (Node v : transNeo.getAllNodes()) {
			for (Relationship e : v.getRelationships(Direction.OUTGOING)) {
				volumeG++;
			}
		}

		// Assume undirected graph
		return volumeG / 2;
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
