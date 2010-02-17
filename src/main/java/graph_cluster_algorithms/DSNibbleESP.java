package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class DSNibbleESP {

	private HashMap<Long, Long> S = new HashMap<Long, Long>(); // Current Set
	private HashMap<Long, Long> B = new HashMap<Long, Long>(); // Boundary Set
	private Long volume = new Long(0);
	private Long cost = new Long(0);
	private Long outDeg = new Long(0);

	public DSNibbleESP(Node x) {
		addNodetoS(x);
		this.volume = deg(x);
		this.cost = this.volume;
	}

	public HashMap<Long, Long> getS() {
		return S;
	}

	public Long getCost() {
		return cost;
	}

	// Select next starting vertex, v, with probability p(v,u)
	// -> p(v,u) =
	// ----> IF {v,u} In E RETURN (1/2)deg(v)
	// ----> IF v==u RETURN 1/2
	// ----> ELSE RETURN 0
	public Node getNextV(Node previousV) {
		ArrayList<Node> neighbours = new ArrayList<Node>();

		for (Relationship e : previousV.getRelationships(Direction.OUTGOING))
			neighbours.add(e.getEndNode());

		Random rand = new Random(System.currentTimeMillis());

		int neighbourCount = neighbours.size();
		int randIndex = rand.nextInt(neighbourCount * 2);

		if (randIndex > neighbourCount)
			return previousV;

		return neighbours.get(randIndex);
	}

	// Conductance(St) = sOutDeg(St) / volume(St)
	public double getConductance() {
		return (double) this.outDeg / (double) this.volume;
	}

	// Populate D, set difference between current St-1 & St
	// -> Iterate over nodes, v, in B
	// -> Lookup probNodeInS(v) & compare with Z
	// -> Meanwhile compute volume(S) & cost(S0,...,St)
	public HashMap<Node, Boolean> computeDVolumeCost(double Z,
			GraphDatabaseService transNeo) {

		HashMap<Node, Boolean> D = new HashMap<Node, Boolean>(); // New Set Diff

		// Iterate over nodes, v, in B
		for (Entry<Long, Long> entry : B.entrySet()) {
			Node v = transNeo.getNodeById(entry.getKey());

			// Lookup probNodeInS(v) & compare with Z
			if (probNodeInS(v) > Z) {
				// Populate D
				D.put(v, true);

				// Compute volume(St)
				if (nodeInS(v) == false)
					this.volume += deg(v);
			} else {
				// Populate D
				D.put(v, false);

				// Compute volume(St)
				if (nodeInS(v) == true)
					this.volume -= deg(v);
			}

		}

		// Compute cost(St)
		// cost = oldCost + SetDiffSt-1St + OutDegSt-1
		this.cost += this.volume + this.outDeg;

		return D;
	}

	public void applyDToS(HashMap<Node, Boolean> D) {
		// FIXME is this all that's necessary?

		// Add/remove vertices in D to/from S
		for (Entry<Node, Boolean> entry : D.entrySet()) {
			if (entry.getValue() == true)
				addNodetoS(entry.getKey());
			else
				removeNodefromS(entry.getKey());
		}
	}

	// Uniform random value from [0,probNodeInS(v)]
	public double getZ(Node v) {
		Random rand = new Random(System.currentTimeMillis());
		return rand.nextDouble() * probNodeInS(v);
	}

	// Call when a single node, v, is added/from to/from set, S
	// ForAll u neighboursOf(v) (incl v)
	// -> Update edgesFromNodetoS(u)
	// -> Determine if nodeInB(u) by examining edgesFromNodeToB(u) & nodeInS(u)
	// -> Add/remove u to/from B
	private void updateB(Node v) {
		// FIXME if correct, more efficient to just B(u)-- for neighboursOf v

		// ForAll u neighboursOf(v) (incl v)

		long edgesFromVToS = 0;
		long edgesFromVToNotS = 0;

		for (Relationship ve : v.getRelationships(Direction.OUTGOING)) {
			Node u = ve.getEndNode();

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
			if ((edgesFromUToS > 0) && (nodeInS(u) == false)) {
				B.put(u.getId(), edgesFromUToS);
				continue;
			}

			// U is in set S [AND] U has edges out of set S --> U in set B
			if ((edgesFromUToNotS > 0) && (nodeInS(u) == true)) {
				B.put(u.getId(), edgesFromUToS);
				continue;
			}

			B.remove(u.getId());
		}

		// Determine if nodeInB(v)
		// By examining edgesFromNodeToS(v) & nodeInS(v)

		// U is not in set S [AND] U has edges into set S --> U in set B
		if ((edgesFromVToS > 0) && (nodeInS(v) == false))
			B.put(v.getId(), edgesFromVToS);

		// U is in set S [AND] U has edges out of set S --> U in set B
		if ((edgesFromVToNotS > 0) && (nodeInS(v) == true))
			B.put(v.getId(), edgesFromVToS);

		B.remove(v.getId());

		// Update out degree of S
		if (nodeInS(v) == true) {
			this.outDeg -= edgesFromVToS;
			this.outDeg += edgesFromVToNotS;
		} else {
			this.outDeg += edgesFromVToS;
			this.outDeg -= edgesFromVToNotS;
		}

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

		if (S.containsKey(v.getId()))
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

		for (Relationship e : v.getRelationships(Direction.OUTGOING))
			deg++;

		return deg;
	}

	// Check if node, v, is in set, S
	private boolean nodeInS(Node v) {
		return S.containsKey(v.getId());
	}

	// Add node, v, to set, S
	// Update boundary, B
	private void addNodetoS(Node v) {
		S.put(v.getId(), null);
		updateB(v);
	}

	// Remove node, v, from set, S
	// Update boundary, B
	private void removeNodefromS(Node v) {
		S.remove(v.getId());
		updateB(v);
	}

}
