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

public class AlgMemDiDiCBalanced {

	private HashMap<Long, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<Long, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	private HashMap<Byte, Long> clusterSizes = null; // Number of nodes in a
	private HashMap<Byte, Boolean> clusterActivated = null; // Number of

	private String databaseDir;
	private ConfDiDiC config = null;

	private MemGraph memGraph = null;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public void start(String databaseDir, ConfDiDiC confDiDiC,
			Supervisor supervisor, MemGraph memGraph) {

		w = new HashMap<Long, ArrayList<Double>>();
		l = new HashMap<Long, ArrayList<Double>>();
		clusterSizes = new HashMap<Byte, Long>();
		clusterActivated = new HashMap<Byte, Boolean>();
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

			// PRINTOUT
			System.out.printf("\tFOS/T [TimeStep:%d, All Nodes]...", timeStep);

			// For Every "Cluster System"
			for (byte c = 0; c < config.getClusterCount(); c++) {

				if (clusterSizes.get(c) > config.getClusterSizeOff())
					clusterActivated.put(c, false);
				else if (clusterSizes.get(c) < config.getClusterSizeOn())
					clusterActivated.put(c, true);

				if (clusterActivated.get(c) == false)
					continue;

				// For Every Node
				for (MemNode v : this.memGraph.getAllNodes()) {

					// FOS/T Primary Diffusion Algorithm
					doFOST(v, c);

				}

			}

			// TODO REMOVE!!
			System.out.println();
			System.out.printf("Total W = %f\nTotal L = %f\n", getTotalW(),
					getTotalL());
			System.out.printf("Min = %d, Max = %d\n",
					config.getClusterSizeOn(), config.getClusterSizeOff());
			// System.out.printf("Clusters = %s\n", getClusterSizes());

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

		for (byte c = 0; c < config.getClusterCount(); c++) {
			clusterSizes.put(c, (long) 0);
			clusterActivated.put(c, true);
		}

		for (MemNode v : this.memGraph.getAllNodes()) {

			byte vColor = v.getColor();

			ArrayList<Double> wV = new ArrayList<Double>();
			ArrayList<Double> lV = new ArrayList<Double>();

			for (byte c = 0; c < config.getClusterCount(); c++) {

				if (vColor == c) {
					wV.add(new Double(config.getDefClusterVal()));
					lV.add(new Double(config.getDefClusterVal()));
					clusterSizes.put(c, clusterSizes.get(c) + 1);
					continue;
				}

				wV.add(new Double(0));
				lV.add(new Double(0));
			}

			w.put(v.getId(), wV);
			l.put(v.getId(), lV);

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

			for (Entry<Long, ArrayList<Double>> wC : w.entrySet()) {

				MemNode memV = this.memGraph.getNode(wC.getKey());

				Byte vNewColor = memV.getColor();

				clusterSizes.put(vNewColor, clusterSizes.get(vNewColor) - 1);

				switch (allocType) {
				case BASE:
					vNewColor = allocateClusterBasic(wC.getValue());
					break;

				case OPT:
					vNewColor = allocateClusterIntdeg(memV, wC.getValue(),
							timeStep);
					break;

				case HYBRID:
					vNewColor = allocateCluster(memV, wC.getValue(), timeStep);
					break;

				}

				clusterSizes.put(vNewColor, clusterSizes.get(vNewColor) + 1);

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

	// MUST call from inside Transaction
	private void doFOST(MemNode v, byte c) {

		ArrayList<Double> lV = l.get(v.getId());
		ArrayList<Double> wV = w.get(v.getId());

		double wVC = wV.get(c);

		int vDeg = v.getNeighbourCount();

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(v, c);

			// FOS/T Primary Diffusion Algorithm
			for (MemRel e : v.getNeighbours()) {

				MemNode u = this.memGraph.getNode(e.getEndNodeId());

				ArrayList<Double> wU = w.get(u.getId());
				double wUC = wU.get(c);

				double diff = alphaE(u, vDeg) * e.getWeight() * (wVC - wUC);

				// NOTE New
				wVC = wVC - (diff / 2.0);
				// NOTE Old
				// wVC = wVC - diff;

				// NOTE New
				wU.set(c, wUC + (diff / 2.0));
			}

			wVC = wVC + lV.get(c);

			wV.set(c, wVC);
		}

	}

	// MUST call from inside Transaction
	private void doFOSB(MemNode v, byte c) {
		ArrayList<Double> lV = l.get(v.getId());

		double lVC = lV.get(c);
		int vDeg = v.getNeighbourCount();

		double bV = benefit(v, c);

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			// FOS/B Diffusion Algorithm
			for (MemRel e : v.getNeighbours()) {

				MemNode u = this.memGraph.getNode(e.getEndNodeId());

				ArrayList<Double> lU = l.get(u.getId());
				double lUC = lU.get(c);

				double diff = alphaE(u, vDeg) * e.getWeight()
						* ((lVC / bV) - (lUC / benefit(u, c)));

				// NOTE New
				lVC = lVC - (diff / 2.0);
				// NOTE Old
				// lVC = lVC - diff;

				// NOTE New
				lU.set(c, lUC + (diff / 2.0));
			}

		}

		lV.set(c, lVC);

	}

	// MUST call from inside Transaction
	private double alphaE(MemNode u, MemNode v) {
		// alphaE = 1/max{deg(u),deg(v)};
		int uDeg = u.getNeighbourCount();
		int vDeg = v.getNeighbourCount();

		int max = Math.max(uDeg, vDeg);

		return 1.0 / (double) max;
	}

	// MUST call from inside Transaction
	// Optimized version. Find vDeg once only
	private double alphaE(MemNode u, double vDeg) {
		// alphaE = 1/max{deg(u),deg(v)};
		double uDeg = u.getNeighbourCount();

		return 1.0 / Math.max(uDeg, vDeg);
	}

	// MUST call from inside Transaction
	private double benefit(MemNode v, byte c) {
		if (v.getColor() == c)
			return config.getBenefitHigh();
		else
			return config.getBenefitLow();
	}

	// MUST call from inside Transaction
	// Switch between algorithms depending on time-step
	private byte allocateCluster(MemNode v, ArrayList<Double> wC, int timeStep) {
		// Choose cluster with largest load vector
		if ((timeStep < config.getHybridSwitchPoint())
				|| (config.getHybridSwitchPoint() == -1))
			return allocateClusterBasic(wC);

		// Optimization to exclude clusters with no connections
		else
			return allocateClusterIntdeg(v, wC, timeStep);
	}

	// Assign to cluster:
	// * Associated with highest load value
	private byte allocateClusterBasic(ArrayList<Double> wC) {
		byte maxC = 0;
		double maxW = 0.0;

		for (byte c = 0; c < wC.size(); c++) {
			if (wC.get(c) > maxW) {
				maxW = wC.get(c);
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
			int timeStep) {

		byte maxC = v.getColor();
		double maxW = 0.0;

		for (byte c = 0; c < wC.size(); c++) {
			if ((wC.get(c) > maxW) && (intDegNotZero(v, c))) {
				maxW = wC.get(c);
				maxC = c;
			}
		}

		return maxC;
	}

	// MUST call from inside Transaction
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

	private String getClusterSizes() {
		String result = "[ ";

		for (byte c = 0; c < config.getClusterCount(); c++) {
			result = String.format("%s%d ", result, clusterSizes.get(c));
		}

		return String.format("%s]", result);
	}

}
