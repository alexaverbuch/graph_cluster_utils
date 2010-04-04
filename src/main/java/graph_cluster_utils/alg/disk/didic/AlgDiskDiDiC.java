package graph_cluster_utils.alg.disk.didic;

import graph_cluster_utils.alg.config.Conf;
import graph_cluster_utils.alg.config.ConfDiDiC;
import graph_cluster_utils.alg.disk.AlgDisk;
import graph_cluster_utils.general.PropNames;
import graph_cluster_utils.supervisor.Supervisor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * Inherits from {@link AlgDisk}. Basic implementation of the DiDiC
 * clustering/partitioning algorithm, computed directly on a Neo4j instance.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class AlgDiskDiDiC extends AlgDisk {

	private HashMap<String, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<String, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	private ConfDiDiC config = null;

	public AlgDiskDiDiC(String databaseDir, Supervisor supervisor) {
		super(databaseDir, supervisor);
		this.w = new HashMap<String, ArrayList<Double>>();
		this.l = new HashMap<String, ArrayList<Double>>();
	}

	@Override
	public void start(Conf config) {
		this.config = (ConfDiDiC) config;

		if (supervisor.isInitialSnapshot()) {
			supervisor.doInitialSnapshot(this.config.getClusterCount(),
					databaseDir);
		}

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		openTransServices();

		initLoadVectors();

		// PRINTOUT
		System.out.println(getTotalVectorsStr());

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.println(getConfigStr());

		for (int timeStep = 0; timeStep < this.config.getMaxIterations(); timeStep++) {

			long timeStepTime = System.currentTimeMillis();

			// PRINTOUT
			System.out.printf("\tFOS/T [TimeStep:%d, All Nodes]...", timeStep);

			Transaction tx = transNeo.beginTx();

			try {
				// For Every Cluster
				for (byte c = 0; c < this.config.getClusterCount(); c++) {

					// For Every Node
					for (Node v : transNeo.getAllNodes()) {

						// FOS/T Primary Diffusion Algorithm
						doFOST(v, c);

					}

				}

				tx.success();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

			// PRINTOUT
			System.out.printf("DiDiC Complete - Time Taken: %s",
					getTimeStr(System.currentTimeMillis() - timeStepTime));

			updateClusterAllocation(timeStep, this.config.getAllocType());

			if (supervisor.isDynamism(timeStep)) {
				closeTransServices();

				// TODO perform insertions/deletions to Neo4j instance
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

			// PRINTOUT
			System.out.printf(getTotalVectorsStr());

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

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {

				byte vColor = (Byte) v.getProperty(PropNames.COLOR);
				String vName = (String) v.getProperty(PropNames.NAME);

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

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
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

			for (Entry<String, ArrayList<Double>> wC : w.entrySet()) {
				String vName = wC.getKey();

				Node v = transIndexService.getNodes(PropNames.NAME, vName)
						.iterator().next();

				Byte vNewColor = (Byte) v.getProperty(PropNames.COLOR);

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

				v.setProperty(PropNames.COLOR, vNewColor);
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

	// MUST call from inside Transaction
	private void doFOST(Node v, byte c) {

		String vName = (String) v.getProperty(PropNames.NAME);

		ArrayList<Double> lV = l.get(vName);
		ArrayList<Double> wV = w.get(vName);

		double wVC = wV.get(c);

		int vDeg = inDeg(v);

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(v, c);

			// FOS/T Primary Diffusion Algorithm
			for (Relationship e : v.getRelationships(Direction.BOTH)) {

				Node u = e.getOtherNode(v);
				ArrayList<Double> wU = w.get(u.getProperty(PropNames.NAME));

				double wVwUDiff = wVC - wU.get(c);

				wVC = wVC - (alphaE(u, vDeg) * weightE(e) * wVwUDiff);
			}

			wVC = wVC + lV.get(c);

			wV.set(c, wVC);
		}

	}

	// MUST call from inside Transaction
	private void doFOSB(Node v, byte c) {
		String vName = (String) v.getProperty(PropNames.NAME);

		ArrayList<Double> lV = l.get(vName);

		double lVC = lV.get(c);
		int vDeg = inDeg(v);
		double bV = benefit(v, c);

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			// FOS/B Diffusion Algorithm
			for (Relationship e : v.getRelationships(Direction.BOTH)) {

				Node u = e.getOtherNode(v);

				ArrayList<Double> lU = l.get(u.getProperty(PropNames.NAME));

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

		for (Relationship e : v.getRelationships(Direction.BOTH)) {
			vDeg++;
		}

		for (Relationship e : u.getRelationships(Direction.BOTH)) {
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

		for (Relationship e : u.getRelationships(Direction.BOTH)) {
			uDeg++;
		}

		return 1.0 / Math.max(uDeg, vDeg);
	}

	// MUST call from inside Transaction
	private double weightE(Relationship e) {
		if (e.hasProperty(PropNames.WEIGHT)) {
			return (Double) e.getProperty(PropNames.WEIGHT);
		} else
			return 1.0;
	}

	// MUST call from inside Transaction
	private double benefit(Node v, byte c) {

		if ((Byte) v.getProperty(PropNames.COLOR) == c)
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
	private byte allocateClusterIntdeg(Node v, ArrayList<Double> wC,
			int timeStep) {

		byte maxC = (Byte) v.getProperty(PropNames.COLOR);
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
		for (Relationship e : v.getRelationships(Direction.BOTH)) {

			Node u = e.getOtherNode(v);
			byte uColor = (Byte) u.getProperty(PropNames.COLOR);

			if (c == uColor)
				return true;

		}

		return false;
	}

	// MUST call from inside Transaction
	private int inDeg(Node v) {
		int vDeg = 0;

		for (Relationship e : v.getRelationships(Direction.BOTH)) {
			vDeg++;
		}

		return vDeg;
	}

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
