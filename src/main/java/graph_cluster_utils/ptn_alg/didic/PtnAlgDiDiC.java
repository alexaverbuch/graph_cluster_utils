package graph_cluster_utils.ptn_alg.didic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.activation.UnsupportedDataTypeException;

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
import graph_cluster_utils.change_log.ChangeOpEnd;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.general.Consts;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;

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
			LinkedBlockingQueue<ChangeOp> changeLog, Migrator migrator) {
		super(transNeo, logger, changeLog, migrator);

		this.w = new LinkedHashMap<Long, ArrayList<Double>>();
		this.l = new LinkedHashMap<Long, ArrayList<Double>>();
		this.rng = new MersenneTwisterRNG(); // Fast & good randomness
		this.uniGen = new ContinuousUniformGenerator(0.0, 1.0, this.rng);
	}

	@Override
	protected void applyChangeLog(int maxChanges, int maxTimeouts) {

		// PRINTOUT
		System.out.printf("Applying change log...");
		long time = System.currentTimeMillis();

		LinkedBlockingQueue<ChangeOp> blockingChangeLog = (LinkedBlockingQueue<ChangeOp>) changeLog;

		Transaction tx = transNeo.beginTx();

		try {

			int timeouts = 0;
			int changes = 0;

			while (changes < maxChanges) {
				ChangeOp changeOp = blockingChangeLog.poll(
						Consts.CHANGELOG_TIMEOUT, TimeUnit.MILLISECONDS);

				if (changeOp == null) {
					timeouts++;
					if (timeouts > maxTimeouts) {
						System.out.printf("TIMED OUT!! ");
						break;
					}

					System.out.printf(".");
					continue;
				}

				changes++;

				if (changeOp.getClass().getName().equals(
						ChangeOpAddNode.class.getName())) {

					processChangeOpAddNode((ChangeOpAddNode) changeOp);
					continue;
				}

				if (changeOp.getClass().getName().equals(
						ChangeOpDeleteNode.class.getName())) {

					processChangeOpDeleteNode((ChangeOpDeleteNode) changeOp);
					continue;
				}

				if (changeOp.getClass().getName().equals(
						ChangeOpAddRelationship.class.getName())) {

					processChangeOpAddRelationship((ChangeOpAddRelationship) changeOp);
					continue;
				}

				if (changeOp.getClass().getName().equals(
						ChangeOpDeleteRelationship.class.getName())) {

					processChangeOpDeleteRelationship((ChangeOpDeleteRelationship) changeOp);
					continue;
				}

				if (changeOp.getClass().getName().equals(
						ChangeOpEnd.class.getName()))
					break;

				String errStr = String.format("%s not supported\n", changeOp
						.getClass().getName());
				throw new UnsupportedDataTypeException(errStr);

			}

			tx.success();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	protected void processChangeOpAddNode(ChangeOpAddNode changeOp) {

		if (transNeo instanceof MemGraph)
			((MemGraph) transNeo).setNextNodeId(changeOp.getNodeId());

		Node node = transNeo.createNode();
		node.setProperty(Consts.NODE_GID, node.getId());
		node.setProperty(Consts.COLOR, changeOp.getColor());
		initLoadVectors(node);
	}

	protected void processChangeOpDeleteNode(ChangeOpDeleteNode changeOp) {

		Node node = transNeo.getNodeById(changeOp.getNodeId());

		diffuseLoadToNeighbours(node);

		l.remove(node.getId());
		w.remove(node.getId());
		node.delete();
	}

	protected void processChangeOpAddRelationship(
			ChangeOpAddRelationship changeOp) {

		Node startNode = transNeo.getNodeById(changeOp.getStartNodeId());
		Node endNode = transNeo.getNodeById(changeOp.getEndNodeId());
		RelationshipType relType = Consts.RelationshipTypes.DEFAULT;

		if (startNode instanceof MemNode)
			((MemNode) startNode).setNextRelId(changeOp.getId());

		Relationship rel = startNode.createRelationshipTo(endNode, relType);
		// TODO Take as parameter in future
		rel.setProperty(Consts.WEIGHT, 1.0);
	}

	protected void processChangeOpDeleteRelationship(
			ChangeOpDeleteRelationship changeOp) {

		Relationship rel = transNeo.getRelationshipById(changeOp.getId());

		rel.delete();
	}

	// NOTE Currently load is diffused EQUALLY (ignores edge weights)
	protected void diffuseLoadToNeighbours(Node node) {

		int neighbourCount = 0;
		for (Relationship rel : node.getRelationships()) {
			neighbourCount++;
		}

		if (neighbourCount == 0)
			return;

		ArrayList<Double> lV = l.get(node.getId());
		ArrayList<Double> wV = w.get(node.getId());

		for (byte i = 0; i < config.getClusterCount(); i++) {
			double lVPerNeighbour = lV.get(i) / neighbourCount;
			double wVPerNeighbour = wV.get(i) / neighbourCount;

			for (Relationship rel : node.getRelationships()) {

				Node otherNode = rel.getOtherNode(node);

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

	protected String getTotalLoadStr() {
		String result = "";

		double totalL = getTotalL();
		double totalW = getTotalW();

		return String.format("%sTotalL = %f, TotalW = %f, Total = %f\n",
				result, totalL, totalW, totalL + totalW);
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

	protected String getVectorAllStr() {
		String result = "";

		for (Long vId : w.keySet()) {
			if (w.containsKey(vId) == false)
				continue;
			result = String.format("%sVertex[%d] \n\tW = %s \n\tL = %s\n",
					result, vId, getWVStr(vId), getLVStr(vId));
		}

		return result;
	}

	protected String getVectorStr(Long[] vIds) {
		String result = "";

		for (Long vId : vIds) {
			if (w.containsKey(vId) == false)
				continue;
			result = String.format("%sVertex[%d] \n\tW = %s \n\tL = %s\n",
					result, vId, getWVStr(vId), getLVStr(vId));
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
