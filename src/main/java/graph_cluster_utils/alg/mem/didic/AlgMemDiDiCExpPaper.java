package graph_cluster_utils.alg.mem.didic;

import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction; //import org.neo4j.index.IndexService;

import graph_cluster_utils.alg.config.Conf;
import graph_cluster_utils.alg.config.ConfDiDiC;
import graph_cluster_utils.alg.mem.AlgMem;
import graph_cluster_utils.general.PropNames;
import graph_cluster_utils.supervisor.Supervisor;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;
import graph_gen_utils.memory_graph.MemRel;

/**
 * Inherits from {@link AlgMem}.
 * 
 * This is intended to be a complete literal implementation of the DiDiC
 * clustering/partitioning algorithm as described by the relevant published
 * papers and pseudo code.
 * 
 * SYNCHRONY: This implementation ensures a high level on synchrony. All nodes compute on
 * the same time step, same diffusion system (cluster/partition), same FOS/T
 * iteration, and same FOS/B iteration at all times.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class AlgMemDiDiCExpPaper extends AlgMem {

	// Load Vec 1
	private HashMap<Long, ArrayList<Double>> w_prev = null;
	// Load Vec 2 ('drain')
	private HashMap<Long, ArrayList<Double>> l_prev = null;

	// Load Vec 1
	private HashMap<Long, ArrayList<Double>> w_curr = null;
	// Load Vec 2 ('drain')
	private HashMap<Long, ArrayList<Double>> l_curr = null;

	private ConfDiDiC config = null;

	public AlgMemDiDiCExpPaper(String databaseDir, Supervisor supervisor,
			MemGraph memGraph) {
		super(databaseDir, supervisor, memGraph);
		this.w_prev = new HashMap<Long, ArrayList<Double>>();
		this.l_prev = new HashMap<Long, ArrayList<Double>>();
		this.w_curr = new HashMap<Long, ArrayList<Double>>();
		this.l_curr = new HashMap<Long, ArrayList<Double>>();
	}

	@Override
	public void start(Conf config) {

		this.config = (ConfDiDiC) config;

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		if (supervisor.isInitialSnapshot()) {
			supervisor.doInitialSnapshot(this.config.getClusterCount(),
					databaseDir);
		}

		openTransServices();

		initLoadVectors();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.println(getConfigStr());

		for (int timeStep = 0; timeStep < this.config.getMaxIterations(); timeStep++) {

			long timeStepTime = System.currentTimeMillis();

			// TODO For debugging only. Remove later
			printLoadState(new Long[] {});

			// PRINTOUT
			System.out.printf("\tFOS/T [TimeStep:%d, All Nodes]...", timeStep);

			// For Every "Cluster System"
			for (byte c = 0; c < this.config.getClusterCount(); c++) {

				// FOS/T Primary Diffusion Algorithm
				doFOST(c);

			}

			// PRINTOUT
			System.out.printf("DiDiC Complete - Time Taken: %s",
					getTimeStr(System.currentTimeMillis() - timeStepTime));

			updateClusterAllocation(timeStep, this.config.getAllocType());

			if (supervisor.isDynamism(timeStep)) {
				closeTransServices();

				// TODO: perform insertions/deletions to Neo4j instance
				supervisor.doDynamism(databaseDir);

				openTransServices();

				// TODO if graph has changed, DiDiC state must be updated
				// TODO put adaptToGraphChanges() in interface?
				// TODO supervisor.doDynamism() returns change log?
				// TODO supervisor.doDynamism() actually updates graph?
			}

			if (supervisor.isPeriodicSnapshot(timeStep)) {
				closeTransServices();

				supervisor.doPeriodicSnapshot(timeStep, this.config
						.getClusterCount(), databaseDir);

				openTransServices();
			}

		}

		closeTransServices();

		if (supervisor.isFinalSnapshot()) {
			// Take a final snapshot here
			supervisor.doFinalSnapshot(this.config.getClusterCount(),
					databaseDir);
		}

		// PRINTOUT
		System.out.printf("DiDiC Complete - Time Taken: %s", getTimeStr(System
				.currentTimeMillis()
				- time));

		System.out.println("*********DiDiC***********\n");
	}

	private void initLoadVectors() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Load Vectors...");

		for (MemNode v : memGraph.getAllNodes()) {

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
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
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

				Node v = transIndexService.getSingleNode(PropNames.NAME, memV
						.getId().toString());

				v.setProperty(PropNames.COLOR, memV.getColor());
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
		// Copy "current" load vectors to "previous" load vectors
		for (Long lV_curr_key : l_curr.keySet()) {

			ArrayList<Double> lV_curr = l_curr.get(lV_curr_key);
			ArrayList<Double> lV_prev = l_prev.get(lV_curr_key);

			for (int i = 0; i < lV_curr.size(); i++) {
				lV_prev.set(i, lV_curr.get(i));
			}

		}
	}

	private void copyWVectors() {
		// Copy "current" load vectors to "previous" load vectors
		for (Long wV_curr_key : w_curr.keySet()) {

			ArrayList<Double> wV_curr = w_curr.get(wV_curr_key);
			ArrayList<Double> wV_prev = w_prev.get(wV_curr_key);

			for (int i = 0; i < wV_curr.size(); i++) {
				wV_prev.set(i, wV_curr.get(i));
			}

		}
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

			MemNode u = memGraph.getNode(e.getEndNodeId());
			byte uColor = u.getColor();

			if (c == uColor)
				return true;

		}

		return false;
	}

	// **************
	// Printout Stuff
	// **************

	private String getConfigStr() {
		return String
				.format(
						"DiDiC [FOST_ITERS=%d, FOSB_ITERS=%d, MAX_CLUSTERS=%d, TIME_STEPS=%d]%n",
						config.getFOSTIterations(), config.getFOSBIterations(),
						config.getClusterCount(), config.getMaxIterations());
	}

	private String getTotalVectorsStr() {
		return String.format("\nTotalL = %f\nTotalW = %f\n\n", getTotalL(),
				getTotalW());
	}

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
