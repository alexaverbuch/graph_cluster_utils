package graph_cluster_algorithms;

import java.util.HashMap;

import org.neo4j.graphdb.Node;

public class DSNibbleESP {

	private HashMap<Long, Long> S = new HashMap<Long, Long>(); // Current Set
	private HashMap<Long, Long> B = new HashMap<Long, Long>(); // Boundary Set
	private HashMap<Long, Long> D = new HashMap<Long, Long>(); // New Set Diff

	public HashMap<Long, Long> getS() {
		return S;
	}

	public HashMap<Long, Long> getB() {
		return B;
	}

	// Degree of node y
	public int degY(Node y) {
		// TODO
		return 0;
	}
	
	public double probXtoY(Node x, Node y) {
		// TODO
		// IF {x,y} In E RETURN (1/2)d(x)
		// IF x==y RETURN 1/2
		// ELSE RETURN 0
		return 0.0;
	}
	
	public double probYinS(Node y) {
		// TODO
		// ForAll y In S
		// -> SUM (1/2)( edgesYtoS(x)/degY(x) + yInS(x) )
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

	public int yInS(Node y) {
		// TODO
		if (S.containsKey(y.getId()))
			return 1;
		else 
			return 0;
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
	private double getZ(Node x) {
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
	private double conductance() {
		// TODO
		return 0.0;
	}
}
