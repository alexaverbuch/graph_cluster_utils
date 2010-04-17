package graph_cluster_utils.alg.didic;

import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import graph_cluster_utils.alg.config.Conf;
import graph_cluster_utils.alg.config.ConfDiDiC;
import graph_cluster_utils.logger.Logger;
import graph_gen_utils.general.Consts;

/**
 * WARNING: Functional, but has had only minimal testing.
 * 
 * Previously called: "AlgMemDiDiCExpBal".
 * 
 * Inherits from {@link AlgDiDiC}.
 * 
 * Experimental implementation of the DiDiC clustering/partitioning algorithm.
 * 
 * Diffusion systems (clusters/partitions) are enabled/disabled based on their
 * size. This is the first step in modifying the DiDiC algorithm to produce
 * clusters/partitions of near-equal size.
 * 
 * Initial results are promising, but more experimenting and modifications are
 * still needed.
 * 
 * SYNCHRONY: This implementation ensures a low level on synchrony. Nodes
 * compute on the same time step at all times. Nodes compute on the same
 * diffusion system (cluster/partition) and same FOS/T iteration most of the
 * time, and very rarely on the same FOS/B iteration. Moreover, some diffusion
 * systems may be disabled completely at times.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class AlgDiDiCBal extends AlgDiDiC {

	private HashMap<Byte, Long> clusterSizes = null; // Number of nodes in a
	private HashMap<Byte, Boolean> clusterActivated = null; // Number of

	public AlgDiDiCBal(GraphDatabaseService transNeo, Logger logger) {
		super(transNeo, logger);
		this.clusterSizes = new HashMap<Byte, Long>();
		this.clusterActivated = new HashMap<Byte, Boolean>();
	}

	@Override
	public void start(Conf config) {
		this.config = (ConfDiDiC) config;

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		if (logger.isInitialSnapshot()) {
			logger.doInitialSnapshot(transNeo, this.config.getClusterCount());
		}

		initLoadVectors();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.println(getConfigStr());

		for (int timeStep = 0; timeStep < this.config.getMaxIterations(); timeStep++) {

			long timeStepTime = System.currentTimeMillis();

			// PRINTOUT
			System.out.printf("\tFOS/T [TimeStep:%d, All Nodes]...", timeStep);

			// For Every "Cluster System"
			for (byte c = 0; c < this.config.getClusterCount(); c++) {

				if (clusterSizes.get(c) > this.config.getClusterSizeOff())
					clusterActivated.put(c, false);
				else if (clusterSizes.get(c) < this.config.getClusterSizeOn())
					clusterActivated.put(c, true);

				if (clusterActivated.get(c) == false)
					continue;

				Transaction tx = transNeo.beginTx();

				try {

					// For Every Node
					for (Node v : transNeo.getAllNodes()) {

						// FOS/T Primary Diffusion Algorithm
						doFOST(v, c);

					}

				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					tx.finish();
				}

			}

			// TODO For debugging purposes. Remove later!
			System.out.println(getTotalVectorsStr());
			System.out.printf("Min = %d, Max = %d\n", this.config
					.getClusterSizeOn(), this.config.getClusterSizeOff());
			System.out.printf("Clusters = %s\n", getClusterSizes());

			// PRINTOUT
			System.out.printf("DiDiC Complete - Time Taken: %s",
					getTimeStr(System.currentTimeMillis() - timeStepTime));

			updateClusterAllocation(timeStep, this.config.getAllocType());

			if (logger.isPeriodicSnapshot(timeStep)) {
				logger.doPeriodicSnapshot(transNeo, timeStep, this.config
						.getClusterCount());
			}

		}

		if (logger.isFinalSnapshot()) {
			// Take a final snapshot here
			logger.doFinalSnapshot(transNeo, this.config.getClusterCount());
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		System.out.println("*********DiDiC***********\n");
	}

	@Override
	protected void initLoadVectors() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Load Vectors...");

		for (byte c = 0; c < config.getClusterCount(); c++) {
			clusterSizes.put(c, (long) 0);
			clusterActivated.put(c, true);
		}

		Transaction tx = transNeo.beginTx();

		try {

			for (Node v : transNeo.getAllNodes()) {

				byte vColor = (Byte) v.getProperty(Consts.COLOR);

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

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	private void doFOST(Node v, byte c) {

		ArrayList<Double> lV = l.get(v.getId());
		ArrayList<Double> wV = w.get(v.getId());

		double wVC = wV.get(c);

		int vDeg = getDeg(v);

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(v, c);

			// FOS/T Primary Diffusion Algorithm
			for (Relationship e : v.getRelationships()) {

				Node u = e.getOtherNode(v);

				ArrayList<Double> wU = w.get(u.getId());
				double wUC = wU.get(c);

				double diff = alphaE(u, vDeg)
						* (Double) e.getProperty(Consts.WEIGHT) * (wVC - wUC);

				wVC = wVC - (diff / 2.0);

				wU.set(c, wUC + (diff / 2.0));
			}

			wVC = wVC + lV.get(c);

			wV.set(c, wVC);
		}

	}

	private void doFOSB(Node v, byte c) {
		ArrayList<Double> lV = l.get(v.getId());

		double lVC = lV.get(c);
		int vDeg = getDeg(v);

		double bV = benefit(v, c);

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			// FOS/B Diffusion Algorithm
			for (Relationship e : v.getRelationships()) {

				Node u = e.getOtherNode(v);

				ArrayList<Double> lU = l.get(u.getId());
				double lUC = lU.get(c);

				double diff = alphaE(u, vDeg)
						* (Double) e.getProperty(Consts.WEIGHT)
						* ((lVC / bV) - (lUC / benefit(u, c)));

				lVC = lVC - (diff / 2.0);

				lU.set(c, lUC + (diff / 2.0));
			}

		}

		lV.set(c, lVC);

	}

	// ***********************************************************
	// *****************PRINT OUTS********************************
	// ***********************************************************

	private String getClusterSizes() {
		String result = "[ ";

		for (byte c = 0; c < config.getClusterCount(); c++) {
			result = String.format("%s%d ", result, clusterSizes.get(c));
		}

		return String.format("%s]", result);
	}

}
