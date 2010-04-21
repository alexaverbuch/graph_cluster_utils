package graph_cluster_utils.ptn_alg.esp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.uncommons.maths.random.ExponentialGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.config.ConfEvoPartition;
import graph_gen_utils.general.Consts;

/**
 * Base class of all Evolving Set Process (ESP) algorithm implementations.
 * Provides common functionality of the various ESP versions.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class PtnAlgEvoPartition extends PtnAlg {

	protected final static Double CONST_B = 5.0;

	protected ConfEvoPartition config = null;

	protected Random rng = null;
	protected ExponentialGenerator expGenB = null;
	protected ExponentialGenerator expGenVertex = null;

	protected ArrayList<Long> nodes = null;

	protected byte clusterColor = -1;

	public PtnAlgEvoPartition(GraphDatabaseService transNeo, Logger logger,
			LinkedBlockingQueue<ChangeOp> changeLog) {
		super(transNeo, logger, changeLog);

		// this.rng = new Random(); // Slow & poor randomness
		this.rng = new MersenneTwisterRNG(); // Fast & good randomness
		// this.rng = new XORShiftRNG(); // Faster & good randomness

		this.expGenB = new ExponentialGenerator(CONST_B, this.rng);
		this.expGenVertex = new ExponentialGenerator(5.0, this.rng);
	}

	@Override
	protected void applyChangeLog(int maxChanges, int maxTimeouts) {
		// Do nothing, this is not a dynamic partitioning algorithm
	}

	// Color all nodes in Dj with color j
	protected void updateClusterAlloc(ArrayList<Long> Dj, Byte j)
			throws Exception {

		for (Long vID : Dj) {

			Node v = transNeo.getNodeById(vID);
			v.setProperty(Consts.COLOR, j);

			if (nodes.remove(vID) == false) {
				throw new Exception(String.format(
						"Could not remove node %d from nodes", vID));
			}

		}

	}

	// Choose random vertex from remaining (unpartitioned) vertices
	// Choose vertex with probability P(X=x) = d(x)/volume(G)
	protected Node getRandomNode() {
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

	protected ArrayList<Long> getNodeDegrees() {
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
	protected Long getVolumeG(Byte color) {
		Long volumeG = new Long(0);

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {
				// Only count nodes that have not yet been partitioned
				if (v.getProperty(Consts.COLOR) == color) {

					for (Relationship e : v.getRelationships(Direction.BOTH)) {
						if (e.getOtherNode(v).getProperty(Consts.COLOR) == color)
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

	protected void allocateUnallocated() {
		Byte defaultColor = (byte) -1;

		while (nodes.size() > 0) {
			Transaction tx = transNeo.beginTx();

			try {

				for (Node v : transNeo.getAllNodes()) {
					// Only count nodes that have not yet been partitioned
					if ((v.getId() != 0)
							&& (v.getProperty(Consts.COLOR) == defaultColor)) {

						for (Relationship e : v
								.getRelationships(Direction.BOTH)) {
							Byte vColor = (Byte) e.getOtherNode(v).getProperty(
									Consts.COLOR);
							if (vColor != defaultColor) {
								v.setProperty(Consts.COLOR, vColor);
								nodes.remove(v.getId());
								break;
							}
						}

					}
				}

				tx.success();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}
		}

	}

	// ***********************************************************
	// *****************INNER CLASSES*****************************
	// ***********************************************************

	protected class ValueComparator implements Comparator<Object> {

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

	// ***********************************************************
	// *****************PRINT OUTS / DEBUGGING********************
	// ***********************************************************

	protected String nodesToStr() {
		String nodesToStr = "[ ";

		for (Long vID : nodes) {
			nodesToStr += String.format("%d ", vID);
		}
		nodesToStr += "]";

		return nodesToStr;
	}

	protected String dToStr(ArrayList<Long> D) {
		String dToStr = "[ ";
		for (Long vID : D) {
			dToStr += String.format("%d ", vID);
		}
		dToStr += "]";

		return dToStr;
	}

}
