package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import org.uncommons.maths.random.ExponentialGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

import graph_cluster_supervisor.Supervisor;

public class AlgNibbleESP {

	// ESP Related
	private String databaseDir;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	private Supervisor supervisor = null;

	public void start(String databaseDir, ConfNibbleESP config,
			Supervisor supervisor) {
		this.databaseDir = databaseDir;
		this.supervisor = supervisor;

		this.supervisor.do_initial_snapshot(-1, this.databaseDir);

		openTransServices();

		evoPartition(config.getTheta(), config.getP());

		closeTransServices();
	}

	private void evoPartition(Double theta, Double p) {

		// Set W(j) = W(0) = V
		Long volumeG = getVolumeG(-1);

		Long m = volumeG / 2; // Undirected

		// Set j = 0
		Integer j = 0;

		// Set conductance = theta/7
		Double conductance = theta / 7;

		// [WHILE] j < 12.m.Ceil( lg(1/p) ) [AND] volumeWj >= (3/4)volumeG
		Double jMax = 12 * m * Math.ceil(Math.log(1.0 / p));
		Long volumeWj = volumeG;
		while ((j < jMax) && (volumeWj >= (3 / 4) * volumeG)) {

			Transaction tx = transNeo.beginTx();

			try {

				System.err.println("0");

				// -> D(j) = evoNibble(G[W(j-1)], conductance)
				ArrayList<Long> Dj = evoNibble(conductance, volumeWj);

				System.err.println("1");

				// -> Set j = j+1
				if (Dj != null) {
					j++;

					// -> W(j) = W(j-1) - D(j)
					updateClusterAlloc(Dj, j);
					volumeWj = getVolumeG(-1);
					tx.success();
				} else {
					System.err.println(String.format("D(%d) == null!", j));
				}

			} catch (Exception ex) {
				System.err.println(ex.toString());
			} finally {
				tx.finish();
			}
		}

		// Set D = D(1) U ... U D(j)
		// NOTE In this implementation vertices are colored rather than removed
		// NOTE There is no need to perform a Union operation
		// TODO Tidy up the unallocated vertices here?
	}

	// // To get balanced cuts, evoCut is replaced by evoNibble
	// // NOT USED. Only added for reference & consistency with paper
	// private void evoCut(Node v, double conductance) {
	//
	// Double T = Math.floor(Math.pow(conductance, -1) / 100.0);
	// Double thetaT = new Double(0);
	// DSNibbleESP sAndB = genSample(v, T, Double.MAX_VALUE, thetaT);
	//
	// }

	private ArrayList<Long> evoNibble(Double conductance, Long volumeG) {

		// T = Floor(conductance^-1 / 100)
		Double T = Math.floor(Math.pow(conductance, -1) / 100.0);

		// thetaT = Sqrt(4.T^-1.log_2(volume(G)) )
		Double log_2_volumeG = Math.log(volumeG) / Math.log(2);
		Double thetaT = Math.sqrt(4.0 * Math.pow(T, -1) * log_2_volumeG);

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

	// Color all nodes in Dj with color j
	private void updateClusterAlloc(ArrayList<Long> Dj, Integer j) {
		for (Long vID : Dj) {
			Node v = transNeo.getNodeById(vID);
			v.setProperty("color", j);
		}
	}

	// Choose random vertex from remaining (unpartitioned) vertices
	// Choose vertex with probability P(X=x) = d(x)/volume(G)
	private Node getRandomNode() {
		// FIXME currently not random at all

		Node randomNode = transIndexService.getNodes("color", new Integer(-1))
				.iterator().next();

		return randomNode;
	}

	// Only count nodes with "color" == color
	private Long getVolumeG(Integer color) {
		Long volumeG = new Long(0);

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {
				if ((v.getId() != 0) && (v.getProperty("color") == color)) {

					for (Relationship e : v
							.getRelationships(Direction.OUTGOING)) {
						if (e.getEndNode().getProperty("color") == color)
							volumeG++;
					}

				}
			}

		} catch (Exception ex) {
			System.err.println(ex.toString());
		} finally {
			tx.finish();
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
