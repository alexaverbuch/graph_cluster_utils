package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
	private final static Double CONST_B = 1.0;

	private String databaseDir;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	private Supervisor supervisor = null;

	private Iterator randomNodeIter = null;
	private Random rng = new MersenneTwisterRNG();
	private ExponentialGenerator gen = null;

	public void start(String databaseDir, ConfNibbleESP config,
			Supervisor supervisor) throws Exception {
		this.databaseDir = databaseDir;
		this.supervisor = supervisor;
		this.rng = new MersenneTwisterRNG();
		this.gen = new ExponentialGenerator(CONST_B, this.rng);

		this.supervisor.do_initial_snapshot(-1, this.databaseDir);

		openTransServices();

		evoPartition(config.getTheta(), config.getP());

		closeTransServices();
	}

	private void evoPartition(Double theta, Double p) throws Exception {

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

		System.err.printf("evoPartition[theta=%f,p=%f]\n", theta, p);
		System.err.printf("            [conductance=%f,jMax=%f,volumeWj=%d]\n",
				conductance, jMax, volumeWj);

		while ((j < jMax) && (volumeWj >= (3 / 4) * volumeG)) {

			System.err.printf("evoPartition[j=%d,jMax=%f,volumeWj=%d]\n", j,
					jMax, volumeWj);

			Transaction tx = transNeo.beginTx();

			try {

				// -> D(j) = evoNibble(G[W(j-1)], conductance)
				ArrayList<Long> Dj = evoNibble(conductance, volumeWj);

				j++;

				// -> Set j = j+1
				if (Dj != null) {
					System.out.println(String.format(
							"\tevoNibble returned. D(%d) != null!", j));

					// -> W(j) = W(j-1) - D(j)
					updateClusterAlloc(Dj, j);

					volumeWj = getVolumeG(-1);

					tx.success();
				} else {
					System.err.println(String.format(
							"\tevoNibble returned. D(%d) == null!", j));
				}

			} catch (Exception ex) {
				System.err.printf("<evoPartition> \n\t%s\n", ex.toString());
				throw ex;
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

	private ArrayList<Long> evoNibble(Double conductance, Long volumeG)
			throws Exception {

		// T = Floor(conductance^-1 / 100)
		Double T = Math.floor(Math.pow(conductance, -1) / 100.0);

		// thetaT = Sqrt(4.T^-1.log_2(volume(G)) )
		Double log_2_volumeG = Math.log(volumeG) / Math.log(2);
		Double thetaT = Math.sqrt(4.0 * Math.pow(T, -1) * log_2_volumeG);

		// Choose random vertex with probability P(X=x) = d(x)/volume(G)
		Node v = getRandomNode();
		if (v == null)
			return null;

		// Choose random budget
		// -> Let jMax = Ceil(log_2(volumeG))
		Double jMax = Math.ceil(log_2_volumeG);
		// -> Let j be an integer in the range [0,jMax]
		// -> Choose budget with probability P(J=j) = constB.2^-j
		// ----> Where constB is a proportionality constant
		// NOTE exponential & mean = 1 is similar to constB.2^-j & constB = 1
		Double j = this.gen.nextValue() * jMax;
		// -> Let Bj = 8.y.2^j
		// ----> Where y = 1 + 4.Sqrt(T.log_2(volumeG))
		Double y = 1 + 4 * Math.sqrt(T * log_2_volumeG);
		Double Bj = 8 * y * Math.pow(2, j);

		System.err.printf("evoNibble[conductance=%f,volumeG=%d]\n",
				conductance, volumeG);
		System.err.printf("         [v=%d,jMax=%f,j=%f,y=%f,Bj=%f]\n", v
				.getId(), jMax, j, y, Bj);

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
	private DSNibbleESP genSample(Node x, Double T, Double B, Double thetaT)
			throws Exception {

		System.err.printf("genSample[x=%d,T=%f,B=%f,thetaT=%f]\n", x.getId(),
				T, B, thetaT);

		DSNibbleESP sAndB = null; // S, B, volume, conductance
		Node X = null; // Current random-walk position @ time t
		Double Z = new Double(0);
		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>();

		// Init
		// -> X = x0 = x
		X = x;
		// -> S = S0 = {x}
		sAndB = new DSNibbleESP(X);

		try {
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
		} catch (Exception ex) {
			System.err.printf("<genSample> \n\t%s\n", ex.toString());
			throw ex;
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
	private Node getRandomNode() throws Exception {
		// FIXME currently not random at all

		if (randomNodeIter == null)
			randomNodeIter = transIndexService.getNodes("color",
					new Integer(-1)).iterator();

		while (randomNodeIter.hasNext()) {
			Node randomNode = (Node) randomNodeIter.next();
			if (randomNode.getId() == 0)
				continue;
			return randomNode;
		}

		// throw new Exception("getRandomNode: No more nodes");
		return null;
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
			System.err.printf("<getVolumeG> \n\t%s\n", ex.toString());
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
