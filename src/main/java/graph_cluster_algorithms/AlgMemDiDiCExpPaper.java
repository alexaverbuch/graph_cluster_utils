package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import graph_cluster_algorithms.configs.ConfDiDiC;
import graph_cluster_algorithms.supervisors.Supervisor;

import graph_gen_utils.graph.MemGraph;
import graph_gen_utils.graph.MemNode;
import graph_gen_utils.graph.MemRel;

public class AlgMemDiDiCExpPaper {

	// Load Vec 1
	private HashMap<Long, ArrayList<Double>> w_prev = null;
	// Load Vec 2 ('drain')
	private HashMap<Long, ArrayList<Double>> l_prev = null;

	// Load Vec 1
	private HashMap<Long, ArrayList<Double>> w_curr = null;
	// Load Vec 2 ('drain')
	private HashMap<Long, ArrayList<Double>> l_curr = null;

	private String databaseDir;
	private ConfDiDiC config = null;

	private MemGraph memGraph = null;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public void start(String databaseDir, ConfDiDiC confDiDiC,
			Supervisor supervisor, MemGraph memGraph) throws Exception {

		w_prev = new HashMap<Long, ArrayList<Double>>();
		l_prev = new HashMap<Long, ArrayList<Double>>();
		w_curr = new HashMap<Long, ArrayList<Double>>();
		l_curr = new HashMap<Long, ArrayList<Double>>();
		this.databaseDir = databaseDir;
		this.config = confDiDiC;
		this.memGraph = memGraph;

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		openTransServices();

		initLoadVectors();

		if (supervisor.isInitialSnapshot()) {
			closeTransServices();
			supervisor.doInitialSnapshot(config.getClusterCount(), databaseDir);
			openTransServices();
		}

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out
				.printf(
						"DiDiC [FOST_ITERS=%d, FOSB_ITERS=%d, MAX_CLUSTERS=%d, TIME_STEPS=%d]%n",
						config.getFOSTIterations(), config.getFOSBIterations(),
						config.getClusterCount(), config.getMaxIterations());

		for (int timeStep = 0; timeStep < config.getMaxIterations(); timeStep++) {

			long timeStepTime = System.currentTimeMillis();

			// FIXME REMOVE
			printLoadState(new Long[] {});

			// PRINTOUT
			System.out.printf("\tFOS/T [TimeStep:%d, All Nodes]...", timeStep);

			// For Every "Cluster System"
			for (byte c = 0; c < config.getClusterCount(); c++) {

				// FOS/T Primary Diffusion Algorithm
				doFOST(c);

			}

			// PRINTOUT
			long msTotal = System.currentTimeMillis() - timeStepTime;
			long ms = msTotal % 1000;
			long s = (msTotal / 1000) % 60;
			long m = (msTotal / 1000) / 60;
			System.out.printf(
					"DiDiC Complete - Time Taken: %d(m):%d(s):%d(ms)%n", m, s,
					ms);

			updateClusterAllocation(timeStep, config.getAllocType());

			if (supervisor.isDynamism(timeStep)) {
				closeTransServices();

				// TODO: perform insertions/deletions to Neo4j instance
				supervisor.doDynamism(this.databaseDir);

				openTransServices();

				// TODO: if graph has changed, DiDiC state must be updated
				// adaptToGraphChanges()

			}

			if (supervisor.isPeriodicSnapshot(timeStep)) {
				closeTransServices();

				supervisor.doPeriodicSnapshot(timeStep, config
						.getClusterCount(), databaseDir);

				openTransServices();
			}

			System.out.printf("\nTotalL = %f\nTotalW = %f\n", getTotalL(),
					getTotalW());

		}

		if (supervisor.isFinalSnapshot()) {
			// TODO: take a final snapshot here
			closeTransServices();

			supervisor.doFinalSnapshot(config.getClusterCount(), databaseDir);

			openTransServices();
		}

		closeTransServices();

		// PRINTOUT
		long msTotal = System.currentTimeMillis() - time;
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;
		System.out.printf("DiDiC Complete - Time Taken: %d(m):%d(s):%d(ms)%n",
				m, s, ms);

		System.out.println("*********DiDiC***********\n");
	}

	private void initLoadVectors() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Load Vectors...");

		for (MemNode v : this.memGraph.getAllNodes()) {

			byte vColor = v.getColor();

			ArrayList<Double> wV_prev = new ArrayList<Double>();
			ArrayList<Double> lV_prev = new ArrayList<Double>();

			ArrayList<Double> wV_curr = new ArrayList<Double>();
			ArrayList<Double> lV_curr = new ArrayList<Double>();

			for (byte c = 0; c < config.getClusterCount(); c++) {

				if (vColor == c) {
					wV_prev.add(new Double(config.getDefClusterVal()));
					lV_prev.add(new Double(config.getDefClusterVal()));

					wV_curr.add(new Double(config.getDefClusterVal()));
					lV_curr.add(new Double(config.getDefClusterVal()));

					continue;
				}

				wV_prev.add(new Double(0));
				lV_prev.add(new Double(0));

				wV_curr.add(new Double(0));
				lV_curr.add(new Double(0));
			}

			w_prev.put(v.getId(), wV_prev);
			l_prev.put(v.getId(), lV_prev);

			w_curr.put(v.getId(), wV_curr);
			l_curr.put(v.getId(), lV_curr);

		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void updateClusterAllocation(int timeStep,
			ConfDiDiC.AllocType allocType) {

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("\tUpdating Cluster Allocation [TimeStep:%d]...",
				timeStep);

		Transaction tx = transNeo.beginTx();

		try {

			for (Long wV_curr_key : w_curr.keySet()) {

				ArrayList<Double> lV_curr = l_curr.get(wV_curr_key);
				ArrayList<Double> wV_curr = w_curr.get(wV_curr_key);

				MemNode memV = memGraph.getNode(wV_curr_key);

				Byte vNewColor = memV.getColor();

				switch (allocType) {
				case BASE:
					vNewColor = allocateClusterBasic(wV_curr, lV_curr);
					break;

				case OPT:
					vNewColor = allocateClusterIntdeg(memV, wV_curr, lV_curr,
							timeStep);
					break;

				case HYBRID:
					vNewColor = allocateClusterHybrid(memV, wV_curr, lV_curr,
							timeStep);
					break;

				}

				memV.setColor(vNewColor);

				Node v = transIndexService.getSingleNode("name", memV.getId()
						.toString());
				v.setProperty("color", memV.getColor());
			}

			tx.success();

		} catch (Exception ex) {
			System.err.printf("<ERR: updateClusterAllocation>");
			System.err.println(ex.toString());
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void doFOST(byte c) {

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// For all nodes: FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(c);

			for (MemNode v : memGraph.getAllNodes()) {

				ArrayList<Double> wV_prev = w_prev.get(v.getId());
				ArrayList<Double> wV_curr = w_curr.get(v.getId());

				double wVC_prev = wV_prev.get(c);
				double wVC_curr = wV_curr.get(c);

				int vDeg = v.getNeighbourCount();

				// For all nodes: FOS/T Primary Diffusion Algorithm
				for (MemRel e : v.getNeighbours()) {

					MemNode u = memGraph.getNode(e.getEndNodeId());

					ArrayList<Double> wU_prev = w_prev.get(u.getId());

					double diff = alphaE(u, vDeg) * e.getWeight()
							* (wVC_prev - wU_prev.get(c));

					wVC_curr = wVC_curr - diff;

				}

				ArrayList<Double> lV_curr = l_curr.get(v.getId());

				wVC_curr = wVC_curr + lV_curr.get(c);
				wV_curr.set(c, wVC_curr);

			}

			copyWVectors();
		}

	}

	private void doFOSB(byte c) {

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			for (MemNode v : memGraph.getAllNodes()) {

				ArrayList<Double> lV_prev = l_prev.get(v.getId());
				ArrayList<Double> lV_curr = l_curr.get(v.getId());

				double lVC_prev = lV_prev.get(c);
				double lVC_curr = lV_curr.get(c);

				int vDeg = v.getNeighbourCount();

				double bV = benefit(v, c);

				// FOS/B Diffusion Algorithm
				for (MemRel e : v.getNeighbours()) {

					MemNode u = memGraph.getNode(e.getEndNodeId());

					ArrayList<Double> lU_prev = l_prev.get(u.getId());

					double lUC_prev = lU_prev.get(c);

					double diff = alphaE(u, vDeg) * e.getWeight()
							* ((lVC_prev / bV) - (lUC_prev / benefit(u, c)));

					lVC_curr = lVC_curr - diff;

				}

				lV_curr.set(c, lVC_curr);

			}

			copyLVectors();

		}

	}

	private void copyLVectors() {
		// System.out.printf("\nBefore TotalL = %f, ", getTotalL());

		// Copy "current" load vectors to "previous" load vectors
		for (Long lV_curr_key : l_curr.keySet()) {

			ArrayList<Double> lV_curr = l_curr.get(lV_curr_key);
			ArrayList<Double> lV_prev = l_prev.get(lV_curr_key);

			for (int i = 0; i < lV_curr.size(); i++) {
				lV_prev.set(i, lV_curr.get(i));
			}

		}

		// System.out.printf("After TotalL = %f\n", getTotalL(), getTotalW());
	}

	private void copyWVectors() {
		// System.out.printf("\nBefore TotalW = %f, \n", getTotalW());

		// Copy "current" load vectors to "previous" load vectors
		for (Long wV_curr_key : w_curr.keySet()) {

			ArrayList<Double> wV_curr = w_curr.get(wV_curr_key);
			ArrayList<Double> wV_prev = w_prev.get(wV_curr_key);

			for (int i = 0; i < wV_curr.size(); i++) {
				wV_prev.set(i, wV_curr.get(i));
			}

		}

		// System.out.printf("After TotalW = %f\n", getTotalW());
	}

	private double alphaE(MemNode u, MemNode v) {
		// alphaE = 1/max{deg(u),deg(v)};
		int uDeg = u.getNeighbourCount();
		int vDeg = v.getNeighbourCount();

		int max = Math.max(uDeg, vDeg);

		return 1.0 / (double) max;
	}

	// Optimized version. Find vDeg once only
	private double alphaE(MemNode u, double vDeg) {
		// alphaE = 1/max{deg(u),deg(v)};
		double uDeg = u.getNeighbourCount();

		return 1.0 / Math.max(uDeg, vDeg);
	}

	private double benefit(MemNode v, byte c) {
		if (v.getColor() == c)
			return config.getBenefitHigh();
		else
			return config.getBenefitLow();
	}

	// Switch between algorithms depending on time-step
	private byte allocateClusterHybrid(MemNode v, ArrayList<Double> wC_curr,
			ArrayList<Double> lC_curr, int timeStep) {
		// Choose cluster with largest load vector
		if ((timeStep < config.getHybridSwitchPoint())
				|| (config.getHybridSwitchPoint() == -1))
			return allocateClusterBasic(wC_curr, lC_curr);

		// Optimization to exclude clusters with no connections
		else
			return allocateClusterIntdeg(v, wC_curr, lC_curr, timeStep);
	}

	// Assign to cluster:
	// * Associated with highest load value
	private byte allocateClusterBasic(ArrayList<Double> wC_curr,
			ArrayList<Double> lC_curr) {
		byte maxC = 0;
		double maxW = 0.0;

		for (byte c = 0; c < wC_curr.size(); c++) {
			if (wC_curr.get(c) > maxW) {
				maxW = wC_curr.get(c);
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
	private byte allocateClusterIntdeg(MemNode v, ArrayList<Double> wC_curr,
			ArrayList<Double> lC_curr, int timeStep) {

		byte maxC = v.getColor();
		double maxW = 0.0;

		for (byte c = 0; c < wC_curr.size(); c++) {
			if ((wC_curr.get(c) > maxW) && (intDegNotZero(v, c))) {
				maxW = wC_curr.get(c);
				maxC = c;
			}
		}

		return maxC;
	}

	// v has at least 1 edge to given cluster
	private boolean intDegNotZero(MemNode v, byte c) {
		for (MemRel e : v.getNeighbours()) {

			MemNode u = this.memGraph.getNode(e.getEndNodeId());
			byte uColor = u.getColor();

			if (c == uColor)
				return true;

		}

		return false;
	}

	// **************
	// Neo4j Services Stuff
	// **************

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

	// **************
	// Printout Stuff
	// **************

	private void printLoadState(Long[] vIds) {
		System.out.printf("***\n");

		double totalL = getTotalL();
		double totalW = getTotalW();

		System.out.printf("TotalL = %f, TotalW = %f, Total = %f\n", totalL,
				totalW, totalL + totalW);

		for (Long vId : vIds) {
			System.out.printf("\tVertex %d \n\t\tW = %s \n\t\tL = %s\n", vId,
					getWV(vId), getLV(vId));
		}

		System.out.printf("***\n");
	}

	private double getTotalW() {
		double result = 0.0;

		for (ArrayList<Double> vW : w_prev.values()) {
			for (Double vWC : vW) {
				result += vWC;
			}
		}

		return result;
	}

	private double getTotalL() {
		double result = 0.0;

		for (ArrayList<Double> vL : l_prev.values()) {
			for (Double vLC : vL) {
				result += vLC;
			}
		}

		return result;
	}

	private String getWV(Long vId) {
		String result = "[";

		for (Double wVC : w_curr.get(vId)) {
			result = String.format("%s %f", result, wVC);
		}

		result = String.format("%s ]", result);

		return result;
	}

	private String getLV(Long vId) {
		String result = "[";

		for (Double lVC : l_curr.get(vId)) {
			result = String.format("%s %f", result, lVC);
		}

		result = String.format("%s ]", result);

		return result;
	}

}
