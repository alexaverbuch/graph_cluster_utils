package graph_cluster_utils.ptn_alg.didic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 * Previously called: "AlgMemDiDiCExpPaper".
 * 
 * Inherits from {@link PtnAlgDiDiC}.
 * 
 * This is intended to be a complete literal implementation of the DiDiC
 * clustering/partitioning algorithm as described by the relevant published
 * papers and pseudo code.
 * 
 * SYNCHRONY: This implementation ensures a high level on synchrony. All nodes
 * compute on the same time step, same diffusion system (cluster/partition),
 * same FOS/T iteration, and same FOS/B iteration at all times.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class PtnAlgDiDiCPaper extends PtnAlgDiDiC {

	// Main Load Vec
	private LinkedHashMap<Long, ArrayList<Double>> w_prev = null;
	// Drain Load Vec
	private LinkedHashMap<Long, ArrayList<Double>> l_prev = null;

	public PtnAlgDiDiCPaper(GraphDatabaseService transNeo, Logger logger,
			Queue<ChangeOp> changeLog) {
		super(transNeo, logger, changeLog);
		this.w_prev = new LinkedHashMap<Long, ArrayList<Double>>();
		this.l_prev = new LinkedHashMap<Long, ArrayList<Double>>();
	}

	@Override
	public void doPartition(Conf config) {

		this.config = (ConfDiDiC) config;

		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		logger.doInitialSnapshot(transNeo, this.config.getClusterCount());

		initLoadVectors();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.println(getConfigStr());

		for (int timeStep = 0; timeStep < this.config.getMaxIterations(); timeStep++) {

			long timeStepTime = System.currentTimeMillis();

			// TODO For debugging only. Remove later
			System.out.println(printLoadStateStr(new Long[] {}));

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

			logger.doPeriodicSnapshot(transNeo, timeStep, this.config
					.getClusterCount());

			applyChangeLog();

		}

		logger.doFinalSnapshot(transNeo, this.config.getClusterCount());

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));

		System.out.println("*********DiDiC***********\n");
	}

	@Override
	protected void initLoadVectors() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Load Vectors...");

		for (Node v : transNeo.getAllNodes()) {

			byte vColor = (Byte) v.getProperty(Consts.COLOR);

			ArrayList<Double> wV_prev = new ArrayList<Double>();
			ArrayList<Double> lV_prev = new ArrayList<Double>();

			ArrayList<Double> wV = new ArrayList<Double>();
			ArrayList<Double> lV = new ArrayList<Double>();

			for (byte c = 0; c < config.getClusterCount(); c++) {

				if (vColor == c) {
					wV_prev.add(new Double(config.getDefClusterVal()));
					lV_prev.add(new Double(config.getDefClusterVal()));

					wV.add(new Double(config.getDefClusterVal()));
					lV.add(new Double(config.getDefClusterVal()));

					continue;
				}

				wV_prev.add(new Double(0));
				lV_prev.add(new Double(0));

				wV.add(new Double(0));
				lV.add(new Double(0));
			}

			w_prev.put(v.getId(), wV_prev);
			l_prev.put(v.getId(), lV_prev);

			w.put(v.getId(), wV);
			l.put(v.getId(), lV);

		}

		// PRINTOUT
		System.out.printf("%s", getTimeStr(System.currentTimeMillis() - time));
	}

	private void doFOST(byte c) {

		for (int fostIter = 0; fostIter < config.getFOSTIterations(); fostIter++) {

			// For all nodes: FOS/B (Secondary/Drain) Diffusion Algorithm
			doFOSB(c);

			Transaction tx = transNeo.beginTx();

			try {

				for (Node v : transNeo.getAllNodes()) {

					ArrayList<Double> wV_prev = w_prev.get(v.getId());
					ArrayList<Double> wV = w.get(v.getId());

					double wVC_prev = wV_prev.get(c);
					double wVC = wV.get(c);

					int vDeg = getDeg(v);

					// For all nodes: FOS/T Primary Diffusion Algorithm
					for (Relationship e : v.getRelationships()) {

						Node u = e.getOtherNode(v);

						ArrayList<Double> wU_prev = w_prev.get(u.getId());

						double diff = alphaE(u, vDeg)
								* (Double) e.getProperty(Consts.WEIGHT)
								* (wVC_prev - wU_prev.get(c));

						wVC = wVC - diff;

					}

					ArrayList<Double> lV = l.get(v.getId());

					wVC = wVC + lV.get(c);
					wV.set(c, wVC);

				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

			copyWVectors();
		}

	}

	private void doFOSB(byte c) {

		for (int fosbIter = 0; fosbIter < config.getFOSBIterations(); fosbIter++) {

			Transaction tx = transNeo.beginTx();

			try {

				for (Node v : transNeo.getAllNodes()) {

					ArrayList<Double> lV_prev = l_prev.get(v.getId());
					ArrayList<Double> lV = l.get(v.getId());

					double lVC_prev = lV_prev.get(c);
					double lVC = lV.get(c);

					int vDeg = getDeg(v);

					double bV = benefit(v, c);

					// FOS/B Diffusion Algorithm
					for (Relationship e : v.getRelationships()) {

						Node u = e.getOtherNode(v);

						ArrayList<Double> lU_prev = l_prev.get(u.getId());

						double lUC_prev = lU_prev.get(c);

						double diff = alphaE(u, vDeg)
								* (Double) e.getProperty(Consts.WEIGHT)
								* ((lVC_prev / bV) - (lUC_prev / benefit(u, c)));

						lVC = lVC - diff;

					}

					lV.set(c, lVC);

				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

			copyLVectors();

		}

	}

	private void copyLVectors() {
		// Copy "current" load vectors to "previous" load vectors
		for (Long lV_key : l.keySet()) {

			ArrayList<Double> lV = l.get(lV_key);
			ArrayList<Double> lV_prev = l_prev.get(lV_key);

			for (int i = 0; i < lV.size(); i++) {
				lV_prev.set(i, lV.get(i));
			}

		}
	}

	private void copyWVectors() {
		// Copy "current" load vectors to "previous" load vectors
		for (Long wV_key : w.keySet()) {

			ArrayList<Double> wV = w.get(wV_key);
			ArrayList<Double> wV_prev = w_prev.get(wV_key);

			for (int i = 0; i < wV.size(); i++) {
				wV_prev.set(i, wV.get(i));
			}

		}
	}

}
