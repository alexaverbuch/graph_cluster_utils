package graph_cluster_utils.ptn_alg.didic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Queue;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.uncommons.maths.random.ContinuousUniformGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.change_log.ChangeOpAddNode;
import graph_cluster_utils.change_log.ChangeOpAddRelationship;
import graph_cluster_utils.change_log.ChangeOpDeleteNode;
import graph_cluster_utils.change_log.ChangeOpDeleteRelationship;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.config.ConfDiDiC;
import graph_cluster_utils.simulation.ChangeOpEnd;
import graph_gen_utils.general.Consts;
import graph_gen_utils.memory_graph.MemGraph;

/**
 * Base class of all DiDiC algorithm implementations. Provides common
 * functionality of the various DiDiC versions.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public abstract class PtnAlgDiDiC extends PtnAlg {

	protected LinkedHashMap<Long, ArrayList<Double>> w = null; // Main Load Vec
	protected LinkedHashMap<Long, ArrayList<Double>> l = null; // Drain Load Vec
	protected Random rng = null;
	private ContinuousUniformGenerator uniGen = null;
	protected ConfDiDiC config = null;

	public PtnAlgDiDiC(GraphDatabaseService transNeo, Logger logger,
			Queue<ChangeOp> changeLog) {
		super(transNeo, logger, changeLog);

		this.w = new LinkedHashMap<Long, ArrayList<Double>>();
		this.l = new LinkedHashMap<Long, ArrayList<Double>>();
		this.rng = new MersenneTwisterRNG(); // Fast & good randomness
		this.uniGen = new ContinuousUniformGenerator(0.0, 1.0, this.rng);
	}

	@Override
	protected void applyChangeLog(int maxChanges) {

		Transaction tx = transNeo.beginTx();

		try {

			int changes = 0;
			ArrayList<Node> deletedNodes = new ArrayList<Node>();

			while ((changeLog.isEmpty() == false) && (changes < maxChanges)) {
				changes++;
				ChangeOp changeOp = changeLog.poll();

				if (changeOp instanceof ChangeOpAddNode) {
					processChangeOpAddNode((ChangeOpAddNode) changeOp);
					continue;
				}

				if (changeOp instanceof ChangeOpDeleteNode) {
					Node deletedNode = processChangeOpDeleteNode(
							(ChangeOpDeleteNode) changeOp, deletedNodes);
					deletedNodes.add(deletedNode);
					continue;
				}

				// FIXME Memory GraphDatabaseService currently NOT a multi-graph
				// FIXME If Relationship A->B added & 1 exists, Relationship
				// A->B will be overwritten (or Exception?)
				if (changeOp instanceof ChangeOpAddRelationship) {
					processChangeOpAddRelationship((ChangeOpAddRelationship) changeOp);
					continue;
				}

				// FIXME Memory GraphDatabaseService currently NOT a multi-graph
				// FIXME If Relationship A->B deleted, ALL A->B Relationships
				// deleted
				if (changeOp instanceof ChangeOpDeleteRelationship) {
					processChangeOpDeleteRelationship((ChangeOpDeleteRelationship) changeOp);
					continue;
				}

				if (changeOp instanceof ChangeOpEnd) {
					break;
				}
			}

			for (Node node : deletedNodes) {
				for (Relationship rel : node.getRelationships()) {
					rel.delete();
				}

				node.delete();
			}

			tx.success();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

	}

	protected void processChangeOpAddNode(ChangeOpAddNode changeOp) {
		if (transNeo instanceof MemGraph)
			((MemGraph) transNeo).setNextId(changeOp.getNodeId());

		Node node = transNeo.createNode();
		Byte nodeColor = (byte) (uniGen.nextValue() * config.getClusterCount());
		node.setProperty(Consts.COLOR, nodeColor);
		initLoadVectors(node);
	}

	protected Node processChangeOpDeleteNode(ChangeOpDeleteNode changeOp,
			ArrayList<Node> deletedNodes) {
		Node node = transNeo.getNodeById(changeOp.getNodeId());
		diffuseLoadToNeighbours(node, deletedNodes);
		return node;
	}

	protected void processChangeOpAddRelationship(
			ChangeOpAddRelationship changeOp) {
		Node startNode = transNeo
				.getNodeById(((ChangeOpAddRelationship) changeOp)
						.getStartNodeId());
		Node endNode = transNeo
				.getNodeById(((ChangeOpAddRelationship) changeOp)
						.getEndNodeId());
		RelationshipType relType = DynamicRelationshipType
				.withName(Consts.DEFAULT_REL_TYPE_STR);
		startNode.createRelationshipTo(endNode, relType);
	}

	protected void processChangeOpDeleteRelationship(
			ChangeOpDeleteRelationship changeOp) {
		Node startNode = transNeo.getNodeById(changeOp.getStartNodeId());
		Node endNode = transNeo.getNodeById(changeOp.getEndNodeId());
		for (Relationship rel : startNode.getRelationships(Direction.OUTGOING)) {

			if (rel.getEndNode().getId() == endNode.getId()) {
				rel.delete();
				break;
			}

		}
	}

	protected void diffuseLoadToNeighbours(Node node,
			ArrayList<Node> deletedNodes) {
		int neighbourCount = 0;
		for (Relationship rel : node.getRelationships()) {
			if (deletedNodes.contains(rel.getOtherNode(node)))
				continue;
			neighbourCount++;
		}

		ArrayList<Double> lV = l.get(node.getId());
		ArrayList<Double> wV = w.get(node.getId());

		for (byte i = 0; i < config.getClusterCount(); i++) {
			double lVPerNeighbour = lV.get(i) / neighbourCount;
			double wVPerNeighbour = wV.get(i) / neighbourCount;

			for (Relationship rel : node.getRelationships()) {
				Node otherNode = rel.getOtherNode(node);
				if (deletedNodes.contains(otherNode))
					continue;
				ArrayList<Double> lVOther = l.get(otherNode.getId());
				ArrayList<Double> wVOther = w.get(otherNode.getId());

				lVOther.set(i, lVOther.get(i) + lVPerNeighbour);
				wVOther.set(i, wVOther.get(i) + wVPerNeighbour);
			}
		}

	}

	protected void initLoadVectorsAll() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Load Vectors...");

		Transaction tx = transNeo.beginTx();

		try {
			for (Node v : transNeo.getAllNodes()) {

				initLoadVectors(v);

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	protected void initLoadVectors(Node v) {
		byte vColor = (Byte) v.getProperty(Consts.COLOR);

		ArrayList<Double> wV = new ArrayList<Double>();
		ArrayList<Double> lV = new ArrayList<Double>();

		for (byte i = 0; i < config.getClusterCount(); i++) {

			if (vColor == i) {
				wV.add(new Double(config.getDefClusterVal()));
				lV.add(new Double(config.getDefClusterVal()));
				continue;
			}

			wV.add(new Double(0));
			lV.add(new Double(0));
		}

		w.put(v.getId(), wV);
		l.put(v.getId(), lV);
	}

	protected void updateClusterAllocationAll(int timeStep,
			ConfDiDiC.AllocType allocType) {

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("\tUpdating Cluster Allocation [TimeStep:%d]...",
				timeStep);

		int lastVisitedIndex = -1;
		int keySetSize = w.keySet().size();

		while (lastVisitedIndex < keySetSize - 1) {

			Transaction tx = transNeo.beginTx();

			try {

				int currentIndex = -1;
				int transBuffer = 0;

				for (Long wCkey : w.keySet()) {

					currentIndex++;

					if (currentIndex < lastVisitedIndex)
						continue;

					lastVisitedIndex = currentIndex;
					transBuffer++;

					updateClusterAllocation(wCkey, timeStep, allocType);

					// Periodic flush to reduce memory consumption
					if (transBuffer % Consts.STORE_BUF == 0) {
						// Commit transaction
						break;
					}

				}

				tx.success();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

	}

	protected void updateClusterAllocation(long nodeId, int timeStep,
			ConfDiDiC.AllocType allocType) {

		Node memV = transNeo.getNodeById(nodeId);

		ArrayList<Double> lV = l.get(nodeId);
		ArrayList<Double> wV = w.get(nodeId);

		Byte vNewColor = (Byte) memV.getProperty(Consts.COLOR);

		switch (allocType) {
		case BASE:
			vNewColor = allocateClusterBasic(wV, lV);
			break;

		case OPT:
			vNewColor = allocateClusterIntdeg(memV, wV, lV, timeStep);
			break;

		case HYBRID:
			vNewColor = allocateCluster(memV, wV, lV, timeStep);
			break;

		}

		memV.setProperty(Consts.COLOR, vNewColor);

	}

	// Optimized version. Find vDeg once only
	protected double alphaE(Node u, int vDeg) {
		// alphaE = 1/max{deg(u),deg(v)};
		double uDeg = getDeg(u);

		return 1.0 / Math.max(uDeg, vDeg);
	}

	protected double benefit(Node v, byte c) {
		byte vColor = (Byte) v.getProperty(Consts.COLOR);
		if (vColor == c)
			return config.getBenefitHigh();
		else
			return config.getBenefitLow();
	}

	// Switch between algorithms depending on time-step
	protected byte allocateCluster(Node v, ArrayList<Double> wC,
			ArrayList<Double> lC, int timeStep) {
		// Choose cluster with largest load vector
		if ((timeStep < config.getHybridSwitchPoint())
				|| (config.getHybridSwitchPoint() == -1))
			return allocateClusterBasic(wC, lC);

		// Optimization to exclude clusters with no connections
		else
			return allocateClusterIntdeg(v, wC, lC, timeStep);
	}

	// Assign to cluster:
	// * Associated with highest load value
	protected byte allocateClusterBasic(ArrayList<Double> wC,
			ArrayList<Double> lC) {
		byte maxC = 0;
		double maxW = 0.0;

		for (byte c = 0; c < wC.size(); c++) {

			double loadTotal = wC.get(c);

			if (loadTotal > maxW) {
				maxW = loadTotal;
				maxC = c;
			}
		}

		return maxC;
	}

	// Optimization to exclude clusters with no connections
	// Assign to cluster:
	// * Associated with highest load value
	// AND
	// * Internal Degree of v is greater than zero
	protected byte allocateClusterIntdeg(Node v, ArrayList<Double> wC,
			ArrayList<Double> lC, int timeStep) {

		byte maxC = (Byte) v.getProperty(Consts.COLOR);
		double maxW = 0.0;

		for (byte c = 0; c < wC.size(); c++) {

			double loadTotal = wC.get(c);

			if ((loadTotal > maxW) && (intDegNotZero(v, c))) {
				maxW = loadTotal;
				maxC = c;
			}
		}

		return maxC;
	}

	// v has at least 1 edge to given cluster
	protected boolean intDegNotZero(Node v, byte c) {
		for (Relationship e : v.getRelationships()) {

			Node u = e.getOtherNode(v);
			byte uColor = (Byte) u.getProperty(Consts.COLOR);

			if (c == uColor)
				return true;

		}

		return false;
	}

	protected int getDeg(Node v) {
		int vDeg = 0;

		for (Relationship e : v.getRelationships()) {
			vDeg++;
		}

		return vDeg;
	}

	// ***********************************************************
	// *****************PRINT OUTS / DEBUGGING********************
	// ***********************************************************

	protected String getConfigStr() {
		return String
				.format(
						"DiDiC [FOST_ITERS=%d, FOSB_ITERS=%d, MAX_CLUSTERS=%d, TIME_STEPS=%d]%n",
						config.getFOSTIterations(), config.getFOSBIterations(),
						config.getClusterCount(), config.getMaxIterations());
	}

	protected String printLoadStateStr(Long[] vIds) {
		String result = "***\n";

		double totalL = getTotalL();
		double totalW = getTotalW();

		result = String.format("%sTotalL = %f, TotalW = %f, Total = %f\n",
				result, totalL, totalW, totalL + totalW);

		for (Long vId : vIds) {
			result = String.format("%s\tVertex %d \n\t\tW = %s \n\t\tL = %s\n",
					result, vId, getWVStr(vId), getLVStr(vId));

		}

		return String.format("%s***\n", result);
	}

	protected String getTotalVectorsStr() {
		return String.format("\nTotalL = %f\nTotalW = %f\n\n", getTotalL(),
				getTotalW());
	}

	protected double getTotalW() {
		double result = 0.0;

		for (ArrayList<Double> vW : w.values()) {
			for (Double vWC : vW) {
				result += vWC;
			}
		}

		return result;
	}

	protected double getTotalL() {
		double result = 0.0;

		for (ArrayList<Double> vL : l.values()) {
			for (Double vLC : vL) {
				result += vLC;
			}
		}

		return result;
	}

	protected String getWVStr(Long vId) {
		String result = "[";

		for (Double wVC : w.get(vId)) {
			result = String.format("%s %f", result, wVC);
		}

		result = String.format("%s ]", result);

		return result;
	}

	protected String getLVStr(Long vId) {
		String result = "[";

		for (Double lVC : l.get(vId)) {
			result = String.format("%s %f", result, lVC);
		}

		result = String.format("%s ]", result);

		return result;
	}

}
