package graph_cluster_utils.alg.esp;

import graph_gen_utils.general.Consts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import org.uncommons.maths.random.ContinuousUniformGenerator;

/**
 * Data structure (and related logic) used by the Evolving Set Process
 * algorithm.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class DataStructEvoPartition {

	private ArrayList<Long> S = new ArrayList<Long>(); // Current Set
	private HashMap<Long, Long> B = new HashMap<Long, Long>(); // Boundary Set
	private Long volume = new Long(0);
	private Long cost = new Long(0);
	private Long outDeg = new Long(0);

	private Random rng;
	private ContinuousUniformGenerator uniGen = null;

	public DataStructEvoPartition(Node x, Random rng) {
		try {
			addNodetoS(x);
		} catch (Exception e) {
			e.printStackTrace();
		}

		this.volume = new Long(0);
		this.cost = this.volume;
		this.rng = rng;
		this.uniGen = new ContinuousUniformGenerator(0.0, 1.0, this.rng);
	}

	public ArrayList<Long> getS() {
		return S;
	}

	public Long getCost() {
		return cost;
	}

	// Conductance(St) = sOutDeg(St) / volume(St)
	public Double getConductance() {
		System.out.printf(
				"\t\t\t\t<DSNibbleESP.getConductance> outDegS[%d], volS[%d]\n",
				outDeg, volume);

		if (outDeg == 0)
			return 0.0;

		return (double) outDeg / (double) volume;
	}

	public Long getVolume() {
		return volume;
	}

	// Select next starting vertex, v, with probability p(v,u)
	// -> p(v,u) =
	// ----> IF {v,u} In E RETURN (1/2)deg(v)
	// ----> IF v==u RETURN 1/2
	// ----> ELSE RETURN 0
	public Node getNextV(Node previousV) {

		ArrayList<Node> neighbours = new ArrayList<Node>();

		for (Relationship e : previousV.getRelationships(Direction.BOTH)) {
			Node u = e.getOtherNode(previousV);
			Byte colorU = (Byte) u.getProperty(Consts.COLOR);
			if (colorU == -1)
				neighbours.add(e.getOtherNode(previousV));
		}

		int neighboursSize = neighbours.size();

		if (neighboursSize == 0)
			return previousV;

		int randIndex = (int) (uniGen.nextValue() * ((neighboursSize * 2) - 1));

		if (randIndex >= neighboursSize)
			return previousV;

		return neighbours.get(randIndex);
	}

	// Uniform random value from [0,probNodeInS(v)]
	// -> Compute probYinS(X)
	// -> Select random threshold Z = [0,probNodeInS(v)]
	public double getZ(Node v) {
		return uniGen.nextValue() * probNodeInS(v);
	}

	// Populate D, set difference between current St-1 & St
	// -> Iterate over nodes, v, in B
	// -> Lookup probNodeInS(v) & compare with Z
	// -> Meanwhile compute volume(S) & cost(S0,...,St)
	public HashMap<Node, Boolean> updateBoundary(double Z,
			GraphDatabaseService transNeo) throws Exception {

		// TODO Investigate further
		// This impl. is consistent with the paper and pseudo code,
		// but author's ref impl. only adds if Z <= 0.5 & removes if Z > 0.5

		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>(); // New Set Diff

		// Iterate over nodes, v, in B
		// Populate D
		for (Entry<Long, Long> entry : B.entrySet()) {
			Node v = transNeo.getNodeById(entry.getKey());

			// Lookup probNodeInS(v) & compare with Z
			if (probNodeInS(v) >= Z) {

				// Add to D only if not already in S
				if (nodeInS(v) == false)
					D.put(v, true);

			} else {

				// Remove from S only if already in S
				if (nodeInS(v) == true)
					D.put(v, false);

			}

		}

		applyDToS(D);

		// Compute cost(St)
		// cost = oldCost + volume(symmetricDiff(St-1,St)) + outDeg(St-1)
		// ---> = oldCost + volume(D) + outDeg(St-1)
		cost += getVolumeD(D) + outDeg;

		return D;
	}

	private long getVolumeD(HashMap<Node, Boolean> D) {
		long result = 0;
		for (Node v : D.keySet()) {
			result += deg(v);
		}
		return result;
	}

	private void applyDToS(HashMap<Node, Boolean> D) throws Exception {
		// Add/remove vertices in D to/from S
		for (Entry<Node, Boolean> entry : D.entrySet()) {
			Node v = entry.getKey();

			if (entry.getValue() == true)
				addNodetoS(v);
			else
				removeNodefromS(entry.getKey());
		}
	}

	// Call when a single node, v, is added to set, S
	// ForAll u neighboursOf(v) (incl v)
	// -> Update edgesFromNodetoS(u)
	// -> Determine if nodeInB(u) by examining edgesFromNodeToS(u) & nodeInS(u)
	// -> Add/remove u to/from B
	private void updateBAfterAdd(Node v) {

		// ForAll u neighboursOf(v) (incl v)

		long edgesFromVToS = 0;
		long degV = deg(v);

		for (Relationship ve : v.getRelationships(Direction.BOTH)) {
			Node u = ve.getOtherNode(v);

			// Only consider vertices that have not been partitioned yet
			Byte colorU = (Byte) u.getProperty(Consts.COLOR);
			if (colorU != -1)
				continue;

			boolean uInS = nodeInS(u);

			// Update out degree of S
			if (uInS == true) {
				// Update edgesFromVToS
				// computeEdgesFromNodeToS(v) works too, but more efficient here
				edgesFromVToS++;
				outDeg--;
			} else
				outDeg++;

			long edgesFromUToS = computeEdgesFromNodeToS(u);
			long degU = deg(u);

			// Determine if nodeInB(u)
			// By examining edgesFromNodeToS(u) & nodeInS(u)

			// U is not in set S [AND] U has edges into set S --> U in set B
			if ((uInS == false) && (edgesFromUToS > 0)) {
				B.put(u.getId(), edgesFromUToS);
				continue;
			}

			// U is in set S [AND] U has edges out of set S --> U in set B
			if ((uInS == true) && (edgesFromUToS < degU)) {
				B.put(u.getId(), edgesFromUToS);
				continue;
			}

			// U is not in set S
			B.remove(u.getId());

		}

		// Determine if nodeInB(v)
		// By examining edgesFromNodeToS(v) & nodeInS(v)

		// V in set S (ALWAYS TRUE) && V has edges out of set S --> V in set B
		if (edgesFromVToS < degV) {
			B.put(v.getId(), edgesFromVToS);
			return;
		}

		// V is not in S
		B.remove(v.getId());

	}

	// Call when a single node, v, is removed from set, S
	// ForAll u neighboursOf(v) (incl v)
	// -> Update edgesFromNodetoS(u)
	// -> Determine if nodeInB(u) by examining edgesFromNodeToS(u) & nodeInS(u)
	// -> Add/remove u to/from B
	private void updateBAfterRemove(Node v) {

		// ForAll u neighboursOf(v) (incl v)

		long edgesFromVToS = 0;

		for (Relationship ve : v.getRelationships(Direction.BOTH)) {
			Node u = ve.getOtherNode(v);

			// Only consider vertices that have not been partitioned yet
			Byte color = (Byte) u.getProperty(Consts.COLOR);
			if (color != -1)
				continue;

			boolean uInS = nodeInS(u);

			if (uInS == true) {
				edgesFromVToS++;
				outDeg--;
			} else
				outDeg++;

			long edgesFromUToS = computeEdgesFromNodeToS(u);
			long degU = deg(u);

			// Determine if nodeInB(u)
			// By examining edgesFromNodeToS(u) & nodeInS(u)

			// U is not in set S [AND] U has edges into set S --> U in set B
			if ((nodeInS(u) == false) && (edgesFromUToS > 0)) {
				B.put(u.getId(), edgesFromUToS);
				continue;
			}

			// U is in set S [AND] U has edges out of set S --> U in set B
			if ((nodeInS(u) == true) && (edgesFromUToS > degU)) {
				B.put(u.getId(), edgesFromUToS);
				continue;
			}

			// U is not in set S
			B.remove(u.getId());

		}

		// Determine if nodeInB(v)
		// By examining edgesFromNodeToS(v) & nodeInS(v)

		// V not in set S (ALWAYS TRUE) && V has edges into set S --> V in set B
		if ((nodeInS(v) == false) && (edgesFromVToS > 0)) {
			B.put(v.getId(), edgesFromVToS);
			return;
		}

		// V is not in S
		B.remove(v.getId());

	}

	// Count edges from node, v, to set, S
	//
	// IF nodeInB(v) RETURN B.value
	// IF nodeNotInB(v) & nodeInS(v) RETURN deg(v)
	// ELSE RETURN 0
	private long edgesFromNodetoS(Node v) {
		Long edgesFromNodeToS = B.get(v.getId());

		if (edgesFromNodeToS != null)
			return edgesFromNodeToS;

		if (S.contains(v.getId()))
			return deg(v);

		return 0;
	}

	// p(v,S) = (1/2)( e(v,S)/d(v) + 1(v IN S) )
	private double probNodeInS(Node v) {
		double nodeInS = 0;
		if (nodeInS(v) == true)
			nodeInS = 1;

		return (0.5) * (((double) edgesFromNodetoS(v) / (double) deg(v)) + nodeInS);
	}

	// Degree of node v
	private long deg(Node v) {
		int deg = 0;

		for (Relationship e : v.getRelationships(Direction.BOTH)) {
			// Only consider vertices that have not been partitioned yet
			Byte colorV = (Byte) e.getOtherNode(v).getProperty(Consts.COLOR);
			if (colorV == -1) {
				deg++;
			}
		}

		return deg;
	}

	// Count edges going from node, v, into set, S
	private long computeEdgesFromNodeToS(Node v) {
		long edgesFromUToS = 0;

		for (Relationship e : v.getRelationships(Direction.BOTH)) {
			if (nodeInS(e.getOtherNode(v)) == true)
				edgesFromUToS++;
		}

		return edgesFromUToS;
	}

	// Check if node, v, is in set, S
	private boolean nodeInS(Node v) {
		return S.contains(v.getId());
	}

	// Add node, v, to set, S
	// Update boundary, B
	private void addNodetoS(Node v) throws Exception {

		if (S.contains(v.getId()) == false) {
			S.add(v.getId());

			// Compute volume(St) = sum( deg(v elementOf St) )
			volume += deg(v);

			updateBAfterAdd(v);
		} else
			throw new Exception(String.format("Node[%d] already in S!", v
					.getId()));
	}

	// Remove node, v, from set, S
	// Update boundary, B
	private void removeNodefromS(Node v) throws Exception {

		if (S.contains(v.getId()) == true) {
			S.remove(v.getId());

			// Compute volume(St) = sum( deg(v elementOf St) )
			volume -= deg(v);

			updateBAfterRemove(v);
		} else
			throw new Exception(String.format("Node[%d] not in S!", v.getId()));

	}

	public void printSAndB() {

		System.out.printf("\t\t\t\t<DSNibbleESP.printSAndB>\n");
		System.out.printf("\t\t\t\tS=[ ");
		for (Long vID : S) {
			System.out.printf("%d ", vID);
		}
		System.out.printf("]\n");

		System.out.printf("\t\t\t\tB=[ ");
		for (Entry<Long, Long> bEntry : B.entrySet()) {
			System.out.printf("(%d,%d) ", bEntry.getKey(), bEntry.getValue());
		}
		System.out.printf("]\n");

	}
}
