package didic_neo4j;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import graph_gen_utils.NeoFromFile;

public class DiDiCPartitioner {

	private static final int FOST_ITERS = 11; // Primary Diffusion
	private static final int FOSB_ITERS = 11; // Secondary Diffusion ('drain')
	private static final int B = 10; // ...
	private static final int MY_CLUSTER_VAL = 100; // Default Init Val

	private HashMap<String, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<String, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	private int clusterCount;
	private String databaseDir;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public static void main(String[] args) {

		try {

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile("var/test-DiDiC");

			// To generate coloured/partitioned neo4j graph
			// * Assign input Chaco graph file & input partitioning file
			neoCreator.generateNeo("graphs/test-DiDiC.graph",
					"partitionings/test-DiDiC.2.ptn");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public DiDiCPartitioner(int clusterCount, String databaseDir) {
		super();
		this.clusterCount = clusterCount;
		this.databaseDir = databaseDir;

		w = new HashMap<String, ArrayList<Double>>();
		l = new HashMap<String, ArrayList<Double>>();
	}

	public void do_DiDiC(int timeSteps) {
		openTransServices();

		init_cluster_allocation();

		init_load_vectors();

		for (int i = 0; i < timeSteps; i++) {

			Transaction tx = transNeo.beginTx();

			try {
				for (Node node : transNeo.getAllNodes()) {
					if (node.getId() != 0) {

						// For Every "Cluster System"
						for (int c = 0; c < clusterCount; c++) {
							// FOS/T Primary Diffusion Algorithm
							do_FOST(node, c);
						}

					}
				}

				update_cluster_allocation();

				// TODO: adaptToGraphChanges <- not sure what this pseudo means

			} catch (Exception ex) {
				System.err.printf("<ERR: DiDiC Outer Loop Aborted>");
			} finally {
				tx.finish();
			}

		}

		closeTransServices();
	}

	private void init_cluster_allocation() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Cluster-Allocation/Colouring...");

		Random rand = new Random();

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {

					try {
						if (node.hasProperty("color") == false)
							node.setProperty("color", rand
									.nextInt(clusterCount));
						else {
							int nodeColor = (Integer) node.getProperty("color");
							if (nodeColor >= clusterCount)
								node.setProperty("color", rand
										.nextInt(clusterCount));
						}
					} catch (Exception e) {
						System.err.printf("[Could not colour node: %d]", node
								.getId());
					}

				}
			}

			tx.success();

		} catch (Exception ex) {
			System.err.printf("<ERR: Colouring Aborted>");
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void init_load_vectors() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Load Vectors...");

		Transaction tx = transNeo.beginTx();

		try {

			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {

					int nodeColor = (Integer) node.getProperty("color");
					String nodeName = (String) node.getProperty("name");

					ArrayList<Double> nodeW = new ArrayList<Double>();
					for (int i = 0; i < clusterCount; i++) {
						if (nodeColor == i) {
							nodeW.add(new Double(100));
							continue;
						}
						nodeW.add(new Double(0));
					}
					w.put(nodeName, nodeW);

					ArrayList<Double> nodeL = new ArrayList<Double>();
					for (int i = 0; i < clusterCount; i++) {
						if (nodeColor == i) {
							nodeL.add(new Double(100));
							continue;
						}
						nodeL.add(new Double(0));
					}
					l.put(nodeName, nodeL);

				}
			}

		} catch (Exception ex) {
			System.err.printf("<ERR: Load Vectors May Not Be Initialised>");
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void do_FOST(Node v, int c) {

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("FOS/T [FOST_ITERS=%d,FOSB_ITERS=%d] ", FOST_ITERS,
				FOSB_ITERS);

		Transaction tx = transNeo.beginTx();

		try {

			String vName = (String) v.getProperty("name");

			// PRINTOUT
			System.out.printf("@ Node %s...", vName);

			for (int fost_iter = 0; fost_iter < FOST_ITERS; fost_iter++) {

				// FOS/B (Secondary/Drain) Diffusion Algorithm
				do_FOSB(v, c);

				ArrayList<Double> lV = l.get(vName);
				ArrayList<Double> wV = w.get(vName);

				// FOS/T Diffusion
				for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

					Node u = e.getEndNode();
					ArrayList<Double> wU = w.get(u.getProperty("name"));

					double wVwUDiff = wV.get(c) - wU.get(c);

					double wVCNew = wV.get(c) - alpha_e(u, v) * weight_e(e)
							* wVwUDiff;

					wV.set(c, wVCNew);
				}

				wV.set(c, wV.get(c) + lV.get(c));
			}

		} catch (Exception ex) {
			System.err.printf("<ERR: do_FOST>");
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	private void do_FOSB(Node v, int c) {
		Transaction tx = transNeo.beginTx();

		try {

			String vName = (String) v.getProperty("name");

			for (int fosb_iter = 0; fosb_iter < FOSB_ITERS; fosb_iter++) {

				ArrayList<Double> lV = l.get(vName);

				// FOS/B Diffusion
				for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

					Node u = e.getEndNode();
					ArrayList<Double> lU = l.get(u.getProperty("name"));

					double lVlUDiff = (lV.get(c) / (double) benefit(v, c))
							- (lU.get(c) / (double) benefit(u, c));

					double lVCNew = lV.get(c) - alpha_e(u, v) * weight_e(e)
							* lVlUDiff;

					lV.set(c, lVCNew);
				}
			}

		} catch (Exception ex) {
			System.err.printf("<ERR: do_FOST>");
		} finally {
			tx.finish();
		}

	}

	// alpha_e = 1/max{deg(u),deg(v)};
	private double alpha_e(Node u, Node v) {
		int uDeg = 0;
		int vDeg = 0;

		Transaction tx = transNeo.beginTx();

		try {
			for (Relationship e : v.getRelationships(Direction.OUTGOING)) {
				vDeg++;
			}

			for (Relationship e : u.getRelationships(Direction.OUTGOING)) {
				uDeg++;
			}
		} catch (Exception ex) {
			System.err.printf("<ERR: alpha_e>");
		} finally {
			tx.finish();
		}

		int max = Math.max(uDeg, vDeg);

		if (max == 0)
			return 1;

		return 1.0 / (double) max;
	}

	private int weight_e(Relationship e) {
		if (e.hasProperty("weight"))
			return (Integer) e.getProperty("weight");
		else
			return 1;
	}

	private int benefit(Node v, int c) {
		int benefit = 1;

		Transaction tx = transNeo.beginTx();

		try {
			int myC = (Integer) v.getProperty("color");
			if (myC == c) {
				benefit = B;
			}
		} catch (Exception ex) {
			System.err.printf("<ERR: benefit>");
		} finally {
			tx.finish();
		}

		return benefit;
	}

	private void update_cluster_allocation() {
		// TODO: implement
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
