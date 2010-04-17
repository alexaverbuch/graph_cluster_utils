package graph_cluster_utils.alg.didic;

import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import graph_cluster_utils.alg.config.Conf;
import graph_cluster_utils.alg.config.ConfDiDiC;
import graph_cluster_utils.logger.Logger;
import graph_gen_utils.general.Consts;

/**
 * WARNING: Not in a usable state!
 * 
 * Previously called: "AlgMemDiDiCExpFix".
 * 
 * Inherits from {@link AlgDiDiC}.
 * 
 * Experimental implementation of the DiDiC clustering/partitioning algorithm.
 * 
 * In the original DiDiC algorithm the sum of load vectors in the primary
 * diffusion system (W) continually increases. As admitted by the authors this
 * is not intentional. This implementation is an ongoing attempt (in
 * collaboration with the authors) into resolving this issue.
 * 
 * More experimenting and modifications are still needed.
 * 
 * SYNCHRONY: N/A at this stage.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class AlgDiDiCFix extends AlgDiDiC {

	// NOTE Experimental
	private double timeStepWeight = 1.0;

	public AlgDiDiCFix(GraphDatabaseService transNeo, Logger logger) {
		super(transNeo, logger);
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

			// NOTE Experimental!
			timeStepWeight = 1.0 + (0.5 * timeStep);

			// TODO For debugging only. Remove later
			// PRINTOUT
			System.out.printf("\nBefore FOST\n");
			System.out.println(printLoadStateStr(new Long[] { (long) 1,
					(long) 100, (long) 500 }));

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

			// TODO For debugging only. Remove later
			// PRINTOUT
			System.out.printf("\nAfter FOST\n");
			System.out.println(printLoadStateStr(new Long[] { (long) 1,
					(long) 100, (long) 500 }));

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

	private void doFOST(byte c) {

		// NOTE Experimental!
		double moveFraction = 0.5;

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// TODO Experimental! Remove later
			// For Every Node: Copy fraction of W back to L
			// for (MemNode v : memGraph.getAllNodes()) {
			// ArrayList<Double> lV = l.get(v.getId());
			// ArrayList<Double> wV = w.get(v.getId());
			// double lVC = lV.get(c);
			// double wVC = wV.get(c);
			// lV.set(c, lVC + (wVC * moveFraction));
			// wV.set(c, wVC - (wVC * moveFraction));
			// }

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(c);

			// NOTE Experimental!
			// For Every Node: Copy fraction of L to W
			for (Long key : w.keySet()) {
				ArrayList<Double> lV = l.get(key);
				ArrayList<Double> wV = w.get(key);
				double lVC = lV.get(c);
				double wVC = wV.get(c);
				lV.set(c, lVC - (lVC * moveFraction));
				wV.set(c, wVC + (lVC * moveFraction));
			}

			// TODO Experimental! Remove later
			// For Every Node: Copy fraction of L to W
			// for (MemNode v : memGraph.getAllNodes()) {
			// ArrayList<Double> lV = l.get(v.getId());
			// ArrayList<Double> wV = w.get(v.getId());
			// double lVC = lV.get(c);
			// double wVC = wV.get(c);
			// lV.set(c, lVC - (lVC * (moveFraction / (1 + moveFraction))));
			// wV.set(c, wVC + (lVC * (moveFraction / (1 + moveFraction))));
			// }

			Transaction tx = transNeo.beginTx();

			try {

				// For Every Node: FOS/T Primary Diffusion Algorithm
				for (Node v : transNeo.getAllNodes()) {

					ArrayList<Double> lV = l.get(v.getId());
					ArrayList<Double> wV = w.get(v.getId());

					double wVC = wV.get(c);

					int vDeg = getDeg(v);

					for (Relationship e : v.getRelationships()) {

						Node u = e.getOtherNode(v);

						ArrayList<Double> wU = w.get(u.getId());

						double diff = alphaE(u, vDeg)
								* (Double) e.getProperty(Consts.WEIGHT)
								* (wVC - wU.get(c));

						wVC = wVC - (diff / 2.0);

						wU.set(c, wU.get(c) + (diff / 2.0));

					}

					wV.set(c, wVC);

				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

			// NOTE Experimental!
			// For Every Node: Copy fraction of W back to L
			for (Long key : w.keySet()) {
				ArrayList<Double> lV = l.get(key);
				ArrayList<Double> wV = w.get(key);
				double lVC = lV.get(c);
				double wVC = wV.get(c);
				lV.set(c, lVC + (wVC * (moveFraction / (1 + moveFraction))));
				wV.set(c, wVC - (wVC * (moveFraction / (1 + moveFraction))));
			}

		}

	}

	private void doFOSB(byte c) {

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			Transaction tx = transNeo.beginTx();

			try {

				// For Every Node: FOS/B Diffusion Algorithm
				for (Node v : transNeo.getAllNodes()) {

					ArrayList<Double> lV = l.get(v.getId());

					double lVC = lV.get(c);
					int vDeg = getDeg(v);

					double bV = benefit(v, c);

					for (Relationship e : v.getRelationships()) {

						Node u = e.getOtherNode(v);

						ArrayList<Double> lU = l.get(u.getId());

						double diff = alphaE(u, vDeg)
								* (Double) e.getProperty(Consts.WEIGHT)
								* ((lVC / bV) - (lU.get(c) / benefit(u, c)));

						lVC = lVC - (diff / 2.0);

						lU.set(c, lU.get(c) + (diff / 2.0));

					}

					lV.set(c, lVC);

				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

		}

	}

	// Assign to cluster:
	// * Associated with highest load value
	@Override
	protected byte allocateClusterBasic(ArrayList<Double> wC,
			ArrayList<Double> lC) {
		byte maxC = 0;
		double maxW = 0.0;

		for (byte c = 0; c < wC.size(); c++) {

			// NOTE Experimental!
			// double loadTotal = wC.get(c)
			// + (lC.get(c) * config.getFOSBIterations());
			// double loadTotal = (2 * wC.get(c)) + lC.get(c);
			// double loadTotal = lC.get(c) / wC.get(c);
			double loadTotal = wC.get(c) + (timeStepWeight * lC.get(c));
			// double loadTotal = wC.get(c) + lC.get(c);

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
	@Override
	protected byte allocateClusterIntdeg(Node v, ArrayList<Double> wC,
			ArrayList<Double> lC, int timeStep) {

		byte maxC = (Byte) v.getProperty(Consts.COLOR);
		double maxW = 0.0;

		for (byte c = 0; c < wC.size(); c++) {

			// NOTE Experimental!
			// double loadTotal = wC.get(c)
			// + (lC.get(c) * config.getFOSBIterations());
			// double loadTotal = (2 * wC.get(c)) + lC.get(c);
			// double loadTotal = lC.get(c) / wC.get(c);
			double loadTotal = wC.get(c) + (timeStepWeight * lC.get(c));
			// double loadTotal = wC.get(c) + lC.get(c);

			if ((loadTotal > maxW) && (intDegNotZero(v, c))) {
				maxW = loadTotal;
				maxC = c;
			}
		}

		return maxC;
	}

}
