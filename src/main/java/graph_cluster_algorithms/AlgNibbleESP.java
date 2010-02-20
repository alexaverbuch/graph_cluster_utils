package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.maths.random.ExponentialGenerator;

import graph_cluster_supervisor.Supervisor;

public class AlgNibbleESP {

	// ESP Related
	private final static Double CONST_B = 5.0;

	private String databaseDir;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	private Supervisor supervisor = null;

	private Iterator<Node> nodeIter = null;
	private MersenneTwisterRNG rng = null;
	private ExponentialGenerator expGenB = null;
	private ExponentialGenerator expGenV = null;

	private ArrayList<Long> nodes = null;

	public void start(String databaseDir, ConfNibbleESP config,
			Supervisor supervisor) throws Exception {
		this.databaseDir = databaseDir;
		this.supervisor = supervisor;
		this.rng = new MersenneTwisterRNG();
		this.expGenB = new ExponentialGenerator(CONST_B, this.rng);
		this.expGenV = new ExponentialGenerator(5.0, this.rng);

		this.supervisor.do_initial_snapshot(-1, this.databaseDir);

		openTransServices();

		this.nodes = getNodeDegrees();

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
		Double jMax = 12 * m * Math.ceil(Math.log(1.0 / p) / Math.log(10));
		Long volumeWj = volumeG;

		System.out.printf("evoPartition[theta=%f,p=%f]\n", theta, p);
		System.out.printf("            [conduct=%f,jMax=%f,volWj=%d]\n",
				conductance, jMax, volumeWj);

		while ((j < jMax) && (volumeWj >= (3 / 4) * volumeG)) {

			System.out.printf("evoPartition[j=%d,jMax=%f,volWj=%d]\n%s\n", j,
					jMax, volumeWj, nodesToStr());

			Transaction tx = transNeo.beginTx();

			try {

				// -> D(j) = evoNibble(G[W(j-1)], conductance)
				ArrayList<Long> Dj = evoNibble(conductance, volumeWj);

				j++;

				// -> Set j = j+1
				if (Dj != null) {
					System.out.printf(
							"\n\tevoNibble returned. |D(%d)| = %d\n\t%s\n\n",
							j, Dj.size(), dToStr(Dj));

					// -> W(j) = W(j-1) - D(j)
					updateClusterAlloc(Dj, j);

					volumeWj = getVolumeG(-1);

					tx.success();
				} else
					System.out.printf(
							"\n\tevoNibble returned. D(%d) = null!\n\n", j);

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
		// Node v = getNonRandomNode();
		Node v = getRandomNode();

		// This can only happen if all nodes have been partitioned
		if (v == null)
			return null;

		// Choose random budget
		// -> Let jMax = Ceil(log_2(volumeG))
		Double jMax = Math.ceil(log_2_volumeG);
		// -> Let j be an integer in the range [0,jMax]
		// -> Choose budget with probability P(J=j) = constB.2^-j
		// ----> Where constB is a proportionality constant
		// NOTE exponential & mean = 1 is similar to constB.2^-j & constB = 1
		Double j = this.expGenB.nextValue() * jMax;
		if (j > jMax) // Exponential distribution -> May return > 1.0
			j = jMax;
		// -> Let Bj = 8.y.2^j
		// ----> Where y = 1 + 4.Sqrt(T.log_2(volumeG))
		Double y = 1 + 4 * Math.sqrt(T * log_2_volumeG);
		Double Bj = 8 * y * Math.pow(2, j);

		System.out.printf("\t\tevoNibble[conduct=%f,volG=%d]\n", conductance,
				volumeG);
		System.out.printf("\t\t         [v=%d,j=%f,jMax=%f,y=%f,Bj=%f]\n", v
				.getId(), j, jMax, y, Bj);

		DSNibbleESP sAndB = genSample(v, T, Bj, thetaT);

		System.out
				.printf(
						"\t\t<evoNibble> genSample() -> S: conductS[%f]<=3thetaT[%f] , volS[%d]<=3/4volG[%f]\n",
						sAndB.getConductance(), 3 * thetaT, sAndB.getVolume(),
						(3.0 / 4.0) * volumeG);

		if ((sAndB.getConductance() <= 3 * thetaT)
				&& (sAndB.getVolume() <= (3.0 / 4.0) * volumeG))
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

		System.out.printf("\t\t\tgenSample[x=%d,T=%f,B=%f,thetaT=%f]\n", x
				.getId(), T, B, thetaT);

		DSNibbleESP sAndB = null; // S, B, volume, conductance
		Node X = null; // Current random-walk position @ time t
		Double Z = new Double(0);
		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>();

		// Init
		// -> X = x0 = x
		X = x;
		// -> S = S0 = {x}
		sAndB = new DSNibbleESP(X, this.rng);

		System.out
				.printf(
						"\t\t\t<genSample> SB_Init: conductS=%f, costS=%d, volS=%d, X=%d\n",
						sAndB.getConductance(), sAndB.getCost(), sAndB
								.getVolume(), X.getId());

		try {
			// ForEach Step t <= T
			for (int t = 0; t < T; t++) {

				// STAGE 1: compute St-1 to St difference

				// -> X = Choose X with p(Xt-1,Xt)
				X = sAndB.getNextV(X);

				// -> Compute probYinS(X)
				// -> Select random threshold Z = getZ(X)
				Z = sAndB.getZ(X);

				System.out.printf(
						"\t\t\t<genSample> t[%d], nextX[%d], Z[%f]\n", t, X
								.getId(), Z);

				// -> Define St = {y | probYinS(y,St-1) > Z}
				// -> D = Set different between St & St-1
				// -> Update volume(St) & cost(S0,...,St)
				D = sAndB.computeDVolumeCost(Z, transNeo);

				// -> IF t==T OR cost()>B RETURN St = St-1 Diff D
				System.out.printf(
						"\t\t\t<genSample> Phase1? costS[%d] > B[%f]\n", sAndB
								.getCost(), B);
				if (sAndB.getCost() > B) {

					System.out.printf(
							"\t\t\t<genSample> t[%d]: Phase[1]: Return\n", t);

					// Add/remove vertices in D to S
					sAndB.applyDToS(D);
					break;
				}

				// STAGE 2: update S to St by adding/removing vertices in D to S

				// -> Add/remove vertices in D to S
				// -> Update B(St-1) to B(St)
				sAndB.applyDToS(D);

				// -> Compute conductance(St) = outDeg(St) / vol(St)
				// -> IF conductance(St) < thetaT RETURN St
				System.out
						.printf(
								"\t\t\t<genSample> Phase2? conductS[%f] < thetaT[%f]\n",
								sAndB.getConductance(), thetaT);
				if (sAndB.getConductance() < thetaT) {

					System.out.printf(
							"\t\t\t<genSample> t[%d]: Phase[2]: Return\n", t);

					break;
				}
			}
		} catch (Exception ex) {
			System.err.printf("<genSample> \n\t%s\n", ex.toString());
			throw ex;
		}

		return sAndB;
	}

	// Color all nodes in Dj with color j
	private void updateClusterAlloc(ArrayList<Long> Dj, Integer j)
			throws Exception {
		for (Long vID : Dj) {
			Node v = transNeo.getNodeById(vID);
			v.setProperty("color", j);

			if (this.nodes.remove(vID) == false) {
				System.out.println(this.nodes.contains(vID));
				throw new Exception(
						"<updateClusterAlloc> Could not remove vID from nodes!");
			}
		}
	}

	// Choose random vertex from remaining (unpartitioned) vertices
	// Choose vertex with probability P(X=x) = d(x)/volume(G)
	private Node getRandomNode() throws Exception {
		Double randVal = this.expGenV.nextValue();

		long randIndex = Math.round(randVal * (this.nodes.size() - 1));

		// Exponential distribution can result in > 1.0
		// Default to node with highest degree in this case
		if (randIndex >= this.nodes.size())
			randIndex = 0;

		return transNeo.getNodeById(this.nodes.get((int) randIndex));
	}

	private ArrayList<Long> getNodeDegrees() throws Exception {
		HashMap<Long, Integer> unsortedNodeDegs = new HashMap<Long, Integer>();
		ValueComparator degreComparator = new ValueComparator(unsortedNodeDegs);
		TreeMap<Long, Integer> sortedNodeDegs = new TreeMap(degreComparator);

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					Integer vDeg = 0;

					for (Relationship e : v
							.getRelationships(Direction.OUTGOING))
						vDeg++;

					unsortedNodeDegs.put(v.getId(), vDeg);

				}
			}

		} catch (Exception ex) {
			System.err.printf("<getNodeDegrees> \n\t%s\n", ex.toString());
			throw ex;
		} finally {
			tx.finish();
		}

		sortedNodeDegs.putAll(unsortedNodeDegs);

		ArrayList<Long> sortedNodes = new ArrayList<Long>();
		for (Long vID : sortedNodeDegs.keySet()) {
			sortedNodes.add(vID);
		}

		// TODO REMOVE!!!
		for (Entry<Long, Integer> nodeDeg : sortedNodeDegs.entrySet()) {
			System.out.printf("*** INDEX[%d] = DEG[%d] ***\n",
					nodeDeg.getKey(), nodeDeg.getValue());
		}

		System.out.printf("\n******\n");

		// TODO REMOVE!!!
		for (Long vID : sortedNodes) {
			System.out.printf("*** INDEX[%d] ***\n", vID);
		}

		return sortedNodes;
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

	private String nodesToStr() {
		String nodesToStr = "[ ";

		for (Long vID : nodes) {
			nodesToStr += String.format("%d ", vID);
		}
		nodesToStr += "]";

		return nodesToStr;
	}

	private String dToStr(ArrayList<Long> D) {
		String dToStr = "[ ";

		for (Long vID : D) {
			dToStr += String.format("%d ", vID);
		}
		dToStr += "]";

		return dToStr;
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

class ValueComparator implements Comparator {

	private Map base;

	public ValueComparator(Map base) {
		this.base = base;
	}

	public int compare(Object a, Object b) {

		if ((Integer) base.get(a) < (Integer) base.get(b)) {
			return 1;
			// } else if ((Integer) base.get(a) == (Integer) base.get(b)) {
			// return 0;
		} else {
			return -1;
		}
	}
}