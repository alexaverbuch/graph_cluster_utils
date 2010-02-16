package graph_cluster_algorithms;

import java.util.HashMap;

import org.neo4j.graphdb.Node;

public class DSNibbleESP {

	private Node Xt = null; // Current random-walk position @ time t
	private HashMap<Long, Long> S = new HashMap<Long, Long>(); // Current Set
	private HashMap<Long, Long> B = new HashMap<Long, Long>(); // Boundary Set
	private HashMap<Long, Long> D = new HashMap<Long, Long>(); // New Set Diff

	public Node getXt() {
		return Xt;
	}

	public void setXt(Node xt) {
		Xt = xt;
	}

	public HashMap<Long, Long> getS() {
		return S;
	}

	public HashMap<Long, Long> getB() {
		return B;
	}

	public double probYinSet(Node y) {
		// TODO
		return 0.0;
	}

	public void addYtoS(Node y) {
		// TODO
	}

	public void removeYfromS(Node y) {
		// TODO
	}

	public int edgesYtoS(Node y) {
		// TODO
		// IF yInB RETURN B.value
		// IF yNotInB & yInS RETURN d(y)
		// ELSE RETURN 0
		return 0;
	}

	public boolean yInS(Node y) {
		// TODO
		return S.containsKey(y.getId());
	}

	// Call when y added/removed from S
	// ForAll z-neighbourOf-y (incl y) in B
	// -> Update edgesZtoS
	// -> Determine if zInB by examining edgesZtoB & zInS
	// -> Add/remove z
	private void updateB(Node y, boolean added) {
		// TODO
	}

	// Uniform random value from [0,probXinS]
	private double getZ() {
		// TODO
		return 0.0;
	}

	// Populate D, set difference between current St-1 & St
	// -> Iterate over B, lookup probYinS, compare with Z
	// -> Meanwhile compute volume(S) & cost(S0,...,St)
	private void populateD() {
		// TODO
	}

	// Conductance(St) = B(St) / volume(St)
	private double computeConductance() {
		// TODO
		return 0.0;
	}
}
