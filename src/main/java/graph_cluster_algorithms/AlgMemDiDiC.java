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

import graph_cluster_supervisor.Supervisor;

import graph_gen_utils.graph.MemGraph;
import graph_gen_utils.graph.MemNode;
import graph_gen_utils.graph.MemRel;

public class AlgMemDiDiC {

	private HashMap<Long, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<Long, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	// private int clusterCount;
	private String databaseDir;
	private ConfDiDiC config = null;

	private MemGraph memGraph = null;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public void start(String databaseDir, ConfDiDiC confDiDiC,
			Supervisor supervisor, MemGraph memGraph) {

		w = new HashMap<Long, ArrayList<Double>>();
		l = new HashMap<Long, ArrayList<Double>>();
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
			for (int c = 0; c < config.getClusterCount(); c++) {

				// For Every Node
				for (MemNode v : this.memGraph.getAllNodes()) {

					// FOS/T Primary Diffusion Algorithm
					doFOST(v, c);

				}

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

			int vColor = v.getColor();

			ArrayList<Double> wV = new ArrayList<Double>();
			ArrayList<Double> lV = new ArrayList<Double>();

			for (int i = 0; i < config.getClusterCount(); i++) {

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

				Integer vNewColor = memV.getColor();

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
	private void doFOST(MemNode v, int c) {

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

				double wVwUDiff = wVC - wU.get(c);

				wVC = wVC - alphaE(u, vDeg) * e.getWeight() * wVwUDiff;

			}

			wVC = wVC + lV.get(c);

			wV.set(c, wVC);
		}

	}

	// MUST call from inside Transaction
	private void doFOSB(MemNode v, int c) {
		ArrayList<Double> lV = l.get(v.getId());

		double lVC = lV.get(c);
		int vDeg = v.getNeighbourCount();

		double bV = benefit(v, c);

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			// FOS/B Diffusion Algorithm
			for (MemRel e : v.getNeighbours()) {

				MemNode u = this.memGraph.getNode(e.getEndNodeId());

				ArrayList<Double> lU = l.get(u.getId());

				double lVlUDiff = (lVC / bV) - (lU.get(c) / benefit(u, c));

				lVC = lVC - (alphaE(u, vDeg) * e.getWeight() * lVlUDiff);

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
	private double benefit(MemNode v, int c) {
		if (v.getColor() == c)
			return config.getBenefitHigh();
		else
			return config.getBenefitLow();
	}

	// MUST call from inside Transaction
	// Switch between algorithms depending on time-step
	private int allocateCluster(MemNode v, ArrayList<Double> wC, int timeStep) {
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
	private int allocateClusterBasic(ArrayList<Double> wC) {
		int maxC = 0;
		double maxW = 0.0;

		for (int c = 0; c < wC.size(); c++) {
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
	private int allocateClusterIntdeg(MemNode v, ArrayList<Double> wC,
			int timeStep) {

		int maxC = v.getColor();
		double maxW = 0.0;

		for (int c = 0; c < wC.size(); c++) {
			if ((wC.get(c) > maxW) && (intDegNotZero(v, c))) {
				maxW = wC.get(c);
				maxC = c;
			}
		}

		return maxC;
	}

	// MUST call from inside Transaction
	// v has at least 1 edge to given cluster
	private boolean intDegNotZero(MemNode v, int c) {
		for (MemRel e : v.getNeighbours()) {

			MemNode u = this.memGraph.getNode(e.getEndNodeId());
			int uColor = u.getColor();

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

}
