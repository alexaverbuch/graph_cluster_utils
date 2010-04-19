package graph_cluster_utils.ptn_alg.didic;

import java.util.ArrayList;
import java.util.Queue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.ptn_alg.config.Conf;
import graph_cluster_utils.ptn_alg.config.ConfDiDiC;
import graph_gen_utils.general.Consts;

/**
 * Previously called: "AlgMemDiDiCExpSync".
 * 
 * Inherits from {@link PtnAlgDiDiC}.
 * 
 * Experimental implementation of the DiDiC clustering/partitioning algorithm.
 * 
 * SYNCHRONY: This implementation ensures a medium level on synchrony. Nodes
 * compute on the same time step at all times. Nodes compute on the same
 * diffusion system (cluster/partition), same FOS/T iteration, and same FOS/B
 * iteration most of the time.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class PtnAlgDiDiCSync extends PtnAlgDiDiC {

	public PtnAlgDiDiCSync(GraphDatabaseService transNeo, Logger logger,
			Queue<ChangeOp> changeLog) {
		super(transNeo, logger, changeLog);
	}

	@Override
	public void doPartition(Conf config) {
		this.config = (ConfDiDiC) config;

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		logger.doInitialSnapshot(transNeo, this.config.getClusterCount());

		initLoadVectorsAll();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.println(getConfigStr());

		for (int timeStep = 0; timeStep < this.config.getMaxIterations(); timeStep++) {

			// TODO For debugging only. Remove later
			System.out.println(printLoadStateStr(new Long[] {}));

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

			updateClusterAllocationAll(timeStep, this.config.getAllocType());

			logger.doPeriodicSnapshot(transNeo, timeStep, this.config
					.getClusterCount());

			applyChangeLog();

		}

		logger.doFinalSnapshot(transNeo, this.config.getClusterCount());

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		System.out.println("*********DiDiC***********\n");
	}

	private void doFOST(byte c) {

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(c);

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

					wVC = wVC + lV.get(c);

					wV.set(c, wVC);

				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

		}

	}

	private void doFOSB(byte c) {

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

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

		}

	}

}
