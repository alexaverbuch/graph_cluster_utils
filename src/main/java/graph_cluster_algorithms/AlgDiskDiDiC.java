package graph_cluster_algorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import graph_cluster_algorithms.configs.ConfDiDiC;
import graph_cluster_algorithms.supervisors.Supervisor;

public class AlgDiskDiDiC {

	private HashMap<String, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<String, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	private String databaseDir;
	ConfDiDiC config = null;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public void start(String databaseDir, ConfDiDiC confDiDiC,
			Supervisor supervisor) {
		w = new HashMap<String, ArrayList<Double>>();
		l = new HashMap<String, ArrayList<Double>>();
		this.databaseDir = databaseDir;
		this.config = confDiDiC;

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		openTransServices();

		initLoadVectors();
		
		System.out.printf("\nTotalL = %f\nTotalW = %f\n\n",getTotalL(), getTotalW());

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

			Transaction tx = transNeo.beginTx();

			try {
				// For Every "Cluster System"
				for (byte c = 0; c < config.getClusterCount(); c++) {

					// TODO outside cluster loop (less frequent node changes)
					// For Every Node
					for (Node v : transNeo.getAllNodes()) {
						if (v.getId() != 0) {

							// FOS/T Primary Diffusion Algorithm
							doFOST(v, c);

						}

					}

				}

				tx.success();

			} catch (Exception ex) {
				System.err.printf("<ERR: DiDiC Outer Loop Aborted>");
				System.err.println(ex.toString());
			} finally {
				tx.finish();
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

			System.out.printf("\nTotalL = %f\nTotalW = %f\n\n",getTotalL(), getTotalW());
			
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

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					byte vColor = (Byte) v.getProperty("color");
					String vName = (String) v.getProperty("name");

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

					w.put(vName, wV);
					l.put(vName, lV);

				}
			}

		} catch (Exception ex) {
			System.err.printf("<ERR: Load Vectors May Not Be Initialised>");
			System.err.println(ex.toString());
		} finally {
			tx.finish();
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

			for (Entry<String, ArrayList<Double>> wC : w.entrySet()) {
				String vName = wC.getKey();

				Node v = transIndexService.getNodes("name", vName).iterator()
						.next();

				Byte vNewColor = (Byte) v.getProperty("color");

				switch (allocType) {
				case BASE:
					vNewColor = allocateClusterBasic(wC.getValue());
					break;

				case OPT:
					vNewColor = allocateClusterIntdeg(v, wC.getValue(),
							timeStep);
					break;

				case HYBRID:
					vNewColor = allocateCluster(v, wC.getValue(), timeStep);
					break;

				}

				v.setProperty("color", vNewColor);
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
	private void doFOST(Node v, byte c) {

		String vName = (String) v.getProperty("name");

		ArrayList<Double> lV = l.get(vName);
		ArrayList<Double> wV = w.get(vName);

		double wVC = wV.get(c);

		int vDeg = inDeg(v);

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(v, c);

			// FOS/T Primary Diffusion Algorithm
			for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

				Node u = e.getEndNode();
				ArrayList<Double> wU = w.get(u.getProperty("name"));

				double wVwUDiff = wVC - wU.get(c);

				wVC = wVC - (alphaE(u, vDeg) * weightE(e) * wVwUDiff);
			}

			wVC = wVC + lV.get(c);

			wV.set(c, wVC);
		}

	}

	// MUST call from inside Transaction
	private void doFOSB(Node v, byte c) {
		String vName = (String) v.getProperty("name");

		ArrayList<Double> lV = l.get(vName);

		double lVC = lV.get(c);
		int vDeg = inDeg(v);
		double bV = benefit(v, c);

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			// FOS/B Diffusion Algorithm
			for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

				Node u = e.getEndNode();

				ArrayList<Double> lU = l.get(u.getProperty("name"));

				double lVlUDiff = (lVC / bV) - (lU.get(c) / benefit(u, c));

				lVC = lVC - (alphaE(u, vDeg) * weightE(e) * lVlUDiff);

			}

		}

		lV.set(c, lVC);

	}

	// MUST call from inside Transaction
	private double alphaE(Node u, Node v) {
		// alphaE = 1/max{deg(u),deg(v)};
		int uDeg = 0;
		int vDeg = 0;

		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {
			vDeg++;
		}

		for (Relationship e : u.getRelationships(Direction.OUTGOING)) {
			uDeg++;
		}

		int max = Math.max(uDeg, vDeg);

		return 1.0 / (double) max;
	}

	// MUST call from inside Transaction
	// Optimized version. Find vDeg once only
	private double alphaE(Node u, double vDeg) {
		// alphaE = 1/max{deg(u),deg(v)};
		double uDeg = 0;

		for (Relationship e : u.getRelationships(Direction.OUTGOING)) {
			uDeg++;
		}

		return 1.0 / Math.max(uDeg, vDeg);
	}

	// MUST call from inside Transaction
	private double weightE(Relationship e) {
		if (e.hasProperty("weight")) {
			return (Double) e.getProperty("weight");
		} else
			return 1.0;
	}

	// MUST call from inside Transaction
	private double benefit(Node v, byte c) {

		if ((Byte) v.getProperty("color") == c)
			return config.getBenefitHigh();
		else
			return config.getBenefitLow();
	}

	// MUST call from inside Transaction
	// Switch between algorithms depending on time-step
	private byte allocateCluster(Node v, ArrayList<Double> wC, int timeStep) {
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
	private byte allocateClusterIntdeg(Node v, ArrayList<Double> wC, int timeStep) {

		byte maxC = (Byte) v.getProperty("color");
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
	private boolean intDegNotZero(Node v, byte c) {
		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

			Node u = e.getEndNode();
			byte uColor = (Byte) u.getProperty("color");

			if (c == uColor)
				return true;

		}

		return false;
	}

	// MUST call from inside Transaction
	private int inDeg(Node v) {
		int vDeg = 0;

		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {
			vDeg++;
		}

		return vDeg;
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

}