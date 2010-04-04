package graph_cluster_utils.alg.disk.esp;

import graph_cluster_utils.alg.config.Conf;
import graph_cluster_utils.alg.config.ConfEvoPartition;
import graph_cluster_utils.alg.disk.AlgDisk;
import graph_cluster_utils.general.PropNames;
import graph_cluster_utils.supervisor.Supervisor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.maths.random.ExponentialGenerator;
import org.uncommons.maths.random.XORShiftRNG;

/**
 * WARNING: Not in a usable state!
 * 
 * Inherits from {@link AlgDisk}.
 * 
 * This is intended to be a complete literal implementation of the Evolving Set
 * Process clustering/partitioning algorithm as described by the relevant
 * published papers and pseudo code.
 * 
 * This implementation is too complex/convoluted to be used in practice. For
 * this reason further development and debugging of this code is pending.
 * 
 * It is computed directly on a Neo4j instance.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class AlgDiskEvoPartition extends AlgDisk {

	private final static Double CONST_B = 5.0;

	private ConfEvoPartition config = null;

	private Random rng = null;
	private ExponentialGenerator expGenB = null;
	private ExponentialGenerator expGenVertex = null;

	private ArrayList<Long> nodes = null;

	private byte clusterColor = -1;

	public AlgDiskEvoPartition(String databaseDir, Supervisor supervisor) {
		super(databaseDir, supervisor);

		// this.rng = new Random(); // Slow & poor randomness
		this.rng = new MersenneTwisterRNG(); // Fast & good randomness
		// this.rng = new XORShiftRNG(); // Faster & good randomness

		this.expGenB = new ExponentialGenerator(CONST_B, this.rng);
		this.expGenVertex = new ExponentialGenerator(5.0, this.rng);

	}

	@Override
	public void start(Conf config) {
		this.config = (ConfEvoPartition) config;

		this.supervisor.doInitialSnapshot(-1, this.databaseDir);

		openTransServices();

		try {
			this.nodes = getNodeDegrees();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// evoPartitionOld(config.getTheta(), config.getP());
		evoPartition(this.config.getConductance(), this.config.getP());

		closeTransServices();

		this.supervisor.doFinalSnapshot(-1, this.databaseDir);
	}

	// p is used to find jMax. Smaller p -> larger jMax
	private void evoPartition(Double conductance, Double p) {

		// Set W(j) = W(0) = V
		Long volumeG = getVolumeG((byte) -1);

		Long m = volumeG / 2;

		// Set j = 0
		Integer j = 0;

		// [WHILE] j < 12.m.Ceil( lg(1/p) ) [AND] volumeWj >= (3/4)volumeG
		Double jMax = 12 * m * Math.ceil(Math.log10(1.0 / p));

		Long volumeWj = volumeG;

		System.out.printf("evoPartition[conduct=%f,p=%f]\n", conductance, p);

		while ((j < jMax) && (volumeWj >= (3.0 / 4.0) * (double) volumeG)) {

			System.out.printf(
					"evoPartition[j=%d,jMax=%f,volWj=%d, 3/4volG[%f]]\n%s\n",
					j, jMax, volumeWj, (3.0 / 4.0) * (double) volumeG,
					nodesToStr());

			Transaction tx = transNeo.beginTx();

			try {

				// -> D(j) = evoNibble(G[W(j-1)], conductance)
				DataStructEvoPartition Dj = evoNibble(conductance, volumeWj);

				// -> Set j = j+1
				j++;

				if (Dj != null) {
					System.out.printf(
							"\n\tevoNibble returned. |D%d|[%d], volD%d[%d]\n",
							j, Dj.getS().size(), j, Dj.getVolume());
					System.out.printf("\t%s\n\n", dToStr(Dj.getS()));

					// -> W(j) = W(j-1) - D(j)
					clusterColor++;
					updateClusterAlloc(Dj.getS(), clusterColor);

					volumeWj -= Dj.getVolume();

					tx.success();
				} else
					System.out.printf(
							"\n\tevoNibble returned. D(%d) = null!\n\n", j);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

		}

		System.out
				.printf(
						"\nevoPartition[volWj=%d, 3/4volG[%f], volG[%d]]\n%s\n\n",
						volumeWj, (3.0 / 4.0) * (double) volumeG, volumeG,
						nodesToStr());

		// Set D = D(1) U ... U D(j)
		// In this implementation vertices are colored rather than removed,
		// So there is no need to perform a Union operation

		// TODO Tidy up the unallocated vertices here?
	}

	// Literal implementation of pseudo code. Too complex/convoluted to be used
	// p is used to find jMax. Smaller p -> larger jMax
	private void evoPartitionOld(Double theta, Double p) {

		// Set W(j) = W(0) = V
		Long volumeG = getVolumeG((byte) -1);

		Long m = volumeG / 2;

		// Set j = 0
		Integer j = 0;

		// Set conductance = theta/7
		Double conductance = theta / 7;

		// [WHILE] j < 12.m.Ceil( lg(1/p) ) [AND] volumeWj >= (3/4)volumeG
		Double jMax = 12 * m * Math.ceil(Math.log10(1.0 / p));

		Long volumeWj = volumeG;

		System.out.printf("evoPartition[theta=%f,p=%f]\n", theta, p);
		System.out.printf("            [conduct=%f,jMax=%f,volWj=%d]\n",
				conductance, jMax, volumeWj);

		while ((j < jMax) && (volumeWj >= (3.0 / 4.0) * (double) volumeG)) {

			System.out.printf(
					"evoPartition[j=%d,jMax=%f,volWj=%d, 3/4volG[%f]]\n%s\n",
					j, jMax, volumeWj, (3.0 / 4.0) * (double) volumeG,
					nodesToStr());

			Transaction tx = transNeo.beginTx();

			try {

				// -> D(j) = evoNibble(G[W(j-1)], conductance)
				DataStructEvoPartition Dj = evoNibble(conductance, volumeWj);

				j++;

				// -> Set j = j+1
				if (Dj != null) {
					System.out.printf(
							"\n\tevoNibble returned. |D%d|[%d], volD%d[%d]\n",
							j, Dj.getS().size(), j, Dj.getVolume());
					System.out.printf("\t%s\n\n", dToStr(Dj.getS()));

					// -> W(j) = W(j-1) - D(j)
					clusterColor++;
					updateClusterAlloc(Dj.getS(), clusterColor);

					volumeWj -= Dj.getVolume();

					tx.success();
				} else
					System.out.printf(
							"\n\tevoNibble returned. D(%d) = null!\n\n", j);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

		}

		System.out
				.printf(
						"\nevoPartition[volWj=%d, 3/4volG[%f], volG[%d]]\n%s\n\n",
						volumeWj, (3.0 / 4.0) * (double) volumeG, volumeG,
						nodesToStr());

		// Set D = D(1) U ... U D(j)
		// In this implementation vertices are colored rather than removed,
		// so there is no need to perform a Union operation

		// TODO Tidy up the unallocated vertices here?
	}

	private DataStructEvoPartition evoNibble(Double conductance, Long volumeWj) {

		// Precompute for efficiency
		Double log2VolumeWj = Math.log(volumeWj) / Math.log(2);
		Double logVolumeWj = Math.log(volumeWj);

		// T = Floor(conductance^-1 / 100)
		Double T = Math.floor(Math.pow(conductance, -1) / 100.0);

		// thetaT = Sqrt(4.T^-1.log(volume(G)) )
		Double thetaT = Math.sqrt(4.0 * Math.pow(T, -1) * logVolumeWj);

		// Choose random vertex with probability P(X=x) = d(x)/volume(G)
		Node v = getRandomNode();

		// This can only happen if all nodes have been partitioned
		if (v == null)
			return null;

		// Choose random budget
		// -> Let jMax = Ceil(log_2(volumeG))
		Double jMax = Math.ceil(log2VolumeWj);

		// -> Let j be an integer in the range [0,jMax]
		// -> Choose budget with probability P(J=j) = constB.2^-j
		// ----> Where constB is a proportionality constant
		// NOTE Exponential distribution has similar charateristics
		Double j = expGenB.nextValue() * jMax;
		if (j > jMax) // Exponential distribution may return > 1.0
			j = 0.0; // If so, default to most probable j value

		// -> Let Bj = 8.y.2^j
		// ----> Where y = 1 + 4.Sqrt(T.log_2(volumeG))
		Double y = 1 + (4 * Math.sqrt(T * logVolumeWj));
		Double Bj = 8 * y * Math.pow(2, j);

		System.out.printf("\t\tevoNibble[conduct=%f,volG=%d]\n", conductance,
				volumeWj);
		System.out.printf("\t\t         [v=%d,j=%f,jMax=%f,y=%f,Bj=%f]\n", v
				.getId(), j, jMax, y, Bj);

		DataStructEvoPartition sAndB = genSample(v, T, Bj, thetaT);

		System.out
				.printf(
						"\t\t<evoNibble> genSample() -> S: conductS[%f]<=3thetaT[%f] , volS[%d]<=3/4volG[%f]\n",
						sAndB.getConductance(), 3 * thetaT, sAndB.getVolume(),
						(3.0 / 4.0) * volumeWj);

		if ((sAndB.getConductance() <= 3 * thetaT)
				&& (sAndB.getVolume() <= (3.0 / 4.0) * volumeWj))
			return sAndB;

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
	private DataStructEvoPartition genSample(Node x, Double T, Double B,
			Double thetaT) {

		System.out.printf("\t\t\tgenSample[x=%d,T=%f,B=%f,thetaT=%f]\n", x
				.getId(), T, B, thetaT);

		DataStructEvoPartition sAndB = null; // S, B, volume, conductance
		Node X = null; // Current random-walk position @ time t
		Double Z = new Double(0);
		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>();

		// Initialization
		// -> X = x0 = x
		X = x;
		// -> S = S0 = {x}
		sAndB = new DataStructEvoPartition(X, rng);

		sAndB.printSAndB();

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
				// -> Select random threshold Z = uniformRand[0,probNodeInS(v)]
				Z = sAndB.getZ(X);

				System.out.printf(
						"\t\t\t<genSample> t[%d], nextX[%d], Z[%f]\n", t, X
								.getId(), Z);

				// -> Define St = {y | probYinS(y,St-1) >= Z}
				// -> D = Set different between St & St-1
				// -> Update volume(St) & cost(S0,...,St)
				// -> Add/remove vertices in D to/from St
				// -> Update B(St-1) to B(St)
				D = sAndB.updateBoundary(Z, transNeo);

				// -> IF t==T OR cost()>B RETURN St = St-1 Diff D
				System.out.printf(
						"\t\t\t<genSample> Phase1? costS[%d] > B[%f]\n", sAndB
								.getCost(), B);
				if (sAndB.getCost() > B) {

					sAndB.printSAndB();

					break;
				}

				// STAGE 2: update S to St by adding/removing vertices in D to S

				// -> Compute conductance(St) = outDeg(St) / vol(St)
				// -> IF conductance(St) < thetaT RETURN St
				System.out
						.printf(
								"\t\t\t<genSample> Phase2? conductS[%f] < thetaT[%f]\n",
								sAndB.getConductance(), thetaT);

				sAndB.printSAndB();

				if (sAndB.getConductance() < thetaT)
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sAndB;
	}

	// Color all nodes in Dj with color j
	private void updateClusterAlloc(ArrayList<Long> Dj, Byte j)
			throws Exception {

		for (Long vID : Dj) {

			Node v = transNeo.getNodeById(vID);
			v.setProperty(PropNames.COLOR, j);

			if (nodes.remove(vID) == false) {
				throw new Exception(String.format(
						"Could not remove node %d from nodes", vID));
			}

		}

	}

	// Choose random vertex from remaining (unpartitioned) vertices
	// Choose vertex with probability P(X=x) = d(x)/volume(G)
	private Node getRandomNode() {
		Double randVal = expGenVertex.nextValue();

		long randIndex = Math.round(randVal * (nodes.size() - 1));

		// Exponential distribution can result in randIndex > 1.0
		// Default to node with highest degree in this case
		if (randIndex >= nodes.size())
			randIndex = 0;

		if (nodes.size() == 0)
			return null;

		return transNeo.getNodeById(nodes.get((int) randIndex));
	}

	private ArrayList<Long> getNodeDegrees() throws Exception {
		HashMap<Long, Integer> unsortedNodeDegs = new HashMap<Long, Integer>();
		ValueComparator degreeComparator = new ValueComparator(unsortedNodeDegs);
		TreeMap<Long, Integer> sortedNodeDegs = new TreeMap<Long, Integer>(
				degreeComparator);

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {

				Integer vDeg = 0;

				for (Relationship e : v.getRelationships(Direction.BOTH))
					vDeg++;

				unsortedNodeDegs.put(v.getId(), vDeg);

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		sortedNodeDegs.putAll(unsortedNodeDegs);

		ArrayList<Long> sortedNodes = new ArrayList<Long>();
		for (Long vID : sortedNodeDegs.keySet()) {
			sortedNodes.add(vID);
		}

		return sortedNodes;
	}

	// Calculate volume of a given colour/partitioned graph
	// volume(G) = sum( deg(v elementOf St) )
	// volume(G) DOES NOT mean edgeCount(G)
	// edgeCount(G) = m = volume(G)/2
	private Long getVolumeG(Byte color) {
		Long volumeG = new Long(0);

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {
				// Only count nodes that have not yet been partitioned
				if (v.getProperty(PropNames.COLOR) == color) {

					for (Relationship e : v.getRelationships(Direction.BOTH)) {
						if (e.getOtherNode(v).getProperty(PropNames.COLOR) == color)
							volumeG++;
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		return volumeG;
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

	class ValueComparator implements Comparator<Object> {

		private Map<Long, Integer> base;

		public ValueComparator(Map<Long, Integer> base) {
			this.base = base;
		}

		public int compare(Object a, Object b) {

			if ((Integer) base.get(a) < (Integer) base.get(b)) {
				return 1;
				// "equal" case is avoided as duplicates should be kept
				// } else if ((Integer) base.get(a) == (Integer) base.get(b)) {
				// return 0;
			} else {
				return -1;
			}
		}
	}

}