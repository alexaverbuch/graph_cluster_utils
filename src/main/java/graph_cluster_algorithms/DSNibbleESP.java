package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import org.uncommons.maths.random.ContinuousUniformGenerator;
import org.uncommons.maths.random.DiscreteUniformGenerator;

public class DSNibbleESP {

	private ArrayList<Long> S = new ArrayList<Long>(); // Current Set
	private HashMap<Long, Long> B = new HashMap<Long, Long>(); // Boundary Set
	private Long volume = new Long(0);
	private Long cost = new Long(0);
	private Long outDeg = new Long(0);

	private Random rng;

	public DSNibbleESP(Node x, Random rng) throws Exception {
		addNodetoS(x);
		this.volume = deg(x);
		this.cost = this.volume;
		this.rng = rng;
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
				this.outDeg, this.volume);

		if (this.outDeg == 0)
			return 0.0;

		return (double) this.outDeg / (double) this.volume;
	}

	public Long getVolume() {
		return this.volume;
	}

	// Select next starting vertex, v, with probability p(v,u)
	// -> p(v,u) =
	// ----> IF {v,u} In E RETURN (1/2)deg(v)
	// ----> IF v==u RETURN 1/2
	// ----> ELSE RETURN 0
	public Node getNextV(Node previousV) {
		ArrayList<Node> neighbours = new ArrayList<Node>();

		for (Relationship e : previousV.getRelationships(Direction.OUTGOING)) {
			Node u = e.getEndNode();
			Integer colorU = (Integer) u.getProperty("color");
			if (colorU == -1)
				neighbours.add(e.getEndNode());
		}

		int neighboursSize = neighbours.size();

		if (neighboursSize == 0)
			return previousV;

		DiscreteUniformGenerator randGen = new DiscreteUniformGenerator(0,
				(neighboursSize * 2) - 1, this.rng);
		int randIndex = randGen.nextValue();

		if (randIndex >= neighboursSize)
			return previousV;

		return neighbours.get(randIndex);
	}

	// Populate D, set difference between current St-1 & St
	// -> Iterate over nodes, v, in B
	// -> Lookup probNodeInS(v) & compare with Z
	// -> Meanwhile compute volume(S) & cost(S0,...,St)
	public HashMap<Node, Boolean> computeAndApplyD(double Z,
			GraphDatabaseService transNeo) throws Exception {

		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>(); // New Set Diff

		// Iterate over nodes, v, in B
		// Populate D
		for (Entry<Long, Long> entry : B.entrySet()) {
			Node v = transNeo.getNodeById(entry.getKey());

			// Lookup probNodeInS(v) & compare with Z
			if (probNodeInS(v) > Z) {

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
		// cost = oldCost + SetDiffSt-1St + OutDegSt-1
		this.cost += this.volume + this.outDeg;

		return D;
	}

	private void applyDToS(HashMap<Node, Boolean> D) throws Exception {
		// Add/remove vertices in D to/from S
		for (Entry<Node, Boolean> entry : D.entrySet()) {
			Node v = entry.getKey();

			if (entry.getValue() == true) {
				addNodetoS(v);

				// Compute volume(St)
				this.volume -= edgesFromNodetoS(v);
				this.volume += deg(v);
			} else {
				removeNodefromS(entry.getKey());

				// Compute volume(St)
				this.volume += edgesFromNodetoS(v);
				this.volume -= deg(v);
			}

		}
	}

	// Uniform random value from [0,probNodeInS(v)]
	public double getZ(Node v) {
		ContinuousUniformGenerator randGen = new ContinuousUniformGenerator(
				0.0, 1.0, this.rng);
		return randGen.nextValue() * probNodeInS(v);
	}

	// Call when a single node, v, is added/from to/from set, S
	// ForAll u neighboursOf(v) (incl v)
	// -> Update edgesFromNodetoS(u)
	// -> Determine if nodeInB(u) by examining edgesFromNodeToB(u) & nodeInS(u)
	// -> Add/remove u to/from B
	private void updateB(Node v) {
		// FIXME if correct, more efficient to "B(u)--" for neighboursOf v?
		// TODO optimize

		// ForAll u neighboursOf(v) (incl v)

		long edgesFromVToS = 0;
		long edgesFromVToNotS = 0;

		for (Relationship ve : v.getRelationships(Direction.OUTGOING)) {
			Node u = ve.getEndNode();

			Integer color = (Integer) u.getProperty("color");

			// NOTE Only consider vertices that have not been partitioned yet
			if (color == -1) {

				if (nodeInS(u) == true)
					edgesFromVToS++;
				else
					edgesFromVToNotS++;

				long edgesFromUToS = 0;
				long edgesFromUToNotS = 0;

				// Update edgesFromNodetoS(u)
				for (Relationship ue : u.getRelationships(Direction.OUTGOING)) {
					if (nodeInS(ue.getEndNode()) == true)
						edgesFromUToS++;
					else
						edgesFromUToNotS++;
				}

				// Determine if nodeInB(u)
				// By examining edgesFromNodeToS(u) & nodeInS(u)

				// U is not in set S [AND] U has edges into set S --> U in set B
				if ((nodeInS(u) == false) && (edgesFromUToS > 0)) {
					B.put(u.getId(), edgesFromUToS);
					continue;
				}

				// U is in set S [AND] U has edges out of set S --> U in set B
				if ((nodeInS(u) == true) && (edgesFromUToNotS > 0)) {
					B.put(u.getId(), edgesFromUToS);
					continue;
				}

				// U is not in set S
				B.remove(u.getId());

			}

		}

		// Update out degree of S
		if (nodeInS(v) == true) {
			// v was just added to S
			this.outDeg -= edgesFromVToS;
			this.outDeg += edgesFromVToNotS;
		} else {
			// v was just removed from S
			this.outDeg += edgesFromVToS;
			this.outDeg -= edgesFromVToNotS;
		}

		// Determine if nodeInB(v)
		// By examining edgesFromNodeToS(v) & nodeInS(v)

		// V is not in set S [AND] V has edges into set S --> V in set B
		if ((nodeInS(v) == false) && (edgesFromVToS > 0)) {
			B.put(v.getId(), edgesFromVToS);
			return;
		}

		// V is in set S [AND] V has edges out of set S --> V in set B
		if ((nodeInS(v) == true) && (edgesFromVToNotS > 0)) {
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

		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {
			// NOTE Only consider vertices that have not been partitioned yet
			Integer color = (Integer) e.getEndNode().getProperty("color");
			if (color == -1) {
				deg++;
			}
		}

		return deg;
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
			updateB(v);
		} else
			throw new Exception(String.format("Node[%d] already in S!", v
					.getId()));
	}

	// Remove node, v, from set, S
	// Update boundary, B
	private void removeNodefromS(Node v) throws Exception {

		if (S.contains(v.getId()) == true) {
			S.remove(v.getId());
			updateB(v);
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
