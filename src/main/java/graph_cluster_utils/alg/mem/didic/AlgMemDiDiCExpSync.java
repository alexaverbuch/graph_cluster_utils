package graph_cluster_utils.alg.mem.didic;

import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

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
 * Experimental implementation of the DiDiC clustering/partitioning algorithm,
 * computed on an in-memory graph.
 * 
 * SYNCHRONY: This implementation ensures a medium level on synchrony. Nodes
 * compute on the same time step at all times. Nodes compute on the same
 * diffusion system (cluster/partition), same FOS/T iteration, and same FOS/B
 * iteration most of the time.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class AlgMemDiDiCExpSync extends AlgMem {

	private HashMap<Long, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<Long, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	private ConfDiDiC config = null;

	public AlgMemDiDiCExpSync(String databaseDir, Supervisor supervisor,
			MemGraph memGraph) {
		super(databaseDir, supervisor, memGraph);

		this.w = new HashMap<Long, ArrayList<Double>>();
		this.l = new HashMap<Long, ArrayList<Double>>();
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

			// TODO For debugging only. Remove later
			printLoadState(new Long[] {});

			long timeStepTime = System.currentTimeMillis();

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

			for (Long wCkey : w.keySet()) {

				MemNode memV = memGraph.getNode(wCkey);

				ArrayList<Double> lV = l.get(wCkey);
				ArrayList<Double> wV = w.get(wCkey);

				Byte vNewColor = memV.getColor();

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

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(c);

			// For Every Node: FOS/T Primary Diffusion Algorithm
			for (MemNode v : memGraph.getAllNodes()) {

				ArrayList<Double> lV = l.get(v.getId());
				ArrayList<Double> wV = w.get(v.getId());

				double wVC = wV.get(c);

				int vDeg = v.getNeighbourCount();

				for (MemRel e : v.getNeighbours()) {

					MemNode u = memGraph.getNode(e.getEndNodeId());

					ArrayList<Double> wU = w.get(u.getId());

					double diff = alphaE(u, vDeg) * e.getWeight()
							* (wVC - wU.get(c));

					wVC = wVC - (diff / 2.0);

					wU.set(c, wU.get(c) + (diff / 2.0));

				}

				wVC = wVC + lV.get(c);

				wV.set(c, wVC);

			}

		}

	}

	private void doFOSB(byte c) {

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			// For Every Node: FOS/B Diffusion Algorithm
			for (MemNode v : memGraph.getAllNodes()) {

				ArrayList<Double> lV = l.get(v.getId());

				double lVC = lV.get(c);
				int vDeg = v.getNeighbourCount();

				double bV = benefit(v, c);

				for (MemRel e : v.getNeighbours()) {

					MemNode u = memGraph.getNode(e.getEndNodeId());

					ArrayList<Double> lU = l.get(u.getId());

					double diff = alphaE(u, vDeg) * e.getWeight()
							* ((lVC / bV) - (lU.get(c) / benefit(u, c)));

					lVC = lVC - (diff / 2.0);

					lU.set(c, lU.get(c) + (diff / 2.0));

				}

				lV.set(c, lVC);

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
	private byte allocateCluster(MemNode v, ArrayList<Double> wC,
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
	private byte allocateClusterBasic(ArrayList<Double> wC, ArrayList<Double> lC) {
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
	private byte allocateClusterIntdeg(MemNode v, ArrayList<Double> wC,
			ArrayList<Double> lC, int timeStep) {

		byte maxC = v.getColor();
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

		for (ArrayList<Double> vW : w.values()) {
			for (Double vWC : vW) {
				result += vWC;
			}
		}

		return result;
	}

	private double getTotalL() {
		double result = 0.0;

		for (ArrayList<Double> vL : l.values()) {
			for (Double vLC : vL) {
				result += vLC;
			}
		}

		return result;
	}

	private String getWV(Long vId) {
		String result = "[";

		for (Double wVC : w.get(vId)) {
			result = String.format("%s %f", result, wVC);
		}

		result = String.format("%s ]", result);

		return result;
	}

	private String getLV(Long vId) {
		String result = "[";

		for (Double lVC : l.get(vId)) {
			result = String.format("%s %f", result, lVC);
		}

		result = String.format("%s ]", result);

		return result;
	}

}
