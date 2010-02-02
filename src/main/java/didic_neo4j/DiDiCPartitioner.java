package didic_neo4j;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

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

	private static final String IN_GRAPH = "test-DiDiC";
	private static final String NEO_NAME = "test-DiDiC";

	public static void main(String[] args) {
		test_DiDiC_no_init(2);
//		test_DiDiC_init(2);
	}

	private static void test_DiDiC_no_init(int clusterCount) {
		try {

			String clusterCountStr = Integer.toString(clusterCount);
			String inNeo = String.format("var/%s", NEO_NAME);
			String inGraph = String.format("graphs/%s.graph", IN_GRAPH);
			String inPtn = String.format("partitionings/%s.%s.ptn", IN_GRAPH,
					clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(inNeo);

			// To generate coloured/partitioned neo4j graph
			// * Assign input Chaco graph file & input partitioning file
			neoCreator.generateNeo(inGraph, inPtn);

			DiDiCPartitioner didic = new DiDiCPartitioner(clusterCount, inNeo);
			didic.do_DiDiC(150, false);

			write_chaco_and_ptn("FINAL", clusterCount);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void test_DiDiC_init(int clusterCount) {
		try {

			String inNeo = String.format("var/%s", NEO_NAME);
			String inGraph = String.format("graphs/%s.graph", IN_GRAPH);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(inNeo);

			// To generate coloured/partitioned neo4j graph
			// * Assign input Chaco graph file & input partitioning file
			neoCreator.generateNeo(inGraph);

			DiDiCPartitioner didic = new DiDiCPartitioner(clusterCount, inNeo);
			didic.do_DiDiC(150, true);

			write_chaco_and_ptn("FINAL", clusterCount);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void write_chaco_and_ptn(String version, int clusterCount) {
		try {

			String clusterCountStr = Integer.toString(clusterCount);
			String inNeo = String.format("var/%s", NEO_NAME, version);
			String outNeo = String.format("var/%s-gen-%s", NEO_NAME, version);
			String outGraph = String.format("graphs/%s-gen-%s.graph", IN_GRAPH,
					version);
			String outPtn = String.format("partitionings/%s-gen-%s.%s.ptn",
					IN_GRAPH, version, clusterCountStr);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreatorIn = new NeoFromFile(inNeo);

			neoCreatorIn.generateChaco(outGraph,
					NeoFromFile.ChacoType.UNWEIGHTED, outPtn);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreatorOut = new NeoFromFile(outNeo);

			// To generate coloured/partitioned neo4j graph
			// * Assign input Chaco graph file & input partitioning file
			neoCreatorOut.generateNeo(outGraph, outPtn);

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

	public void do_DiDiC(int maxTimeSteps, boolean initClusters) {
		// PRINTOUT
		System.out.println("\n*********DiDiC***********");

		openTransServices();

		if (initClusters)
			init_cluster_allocation();

		init_load_vectors();

		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out
				.printf(
						"DiDiC [FOST_ITERS=%d, FOSB_ITERS=%d, MAX_CLUSTERS=%d, TIME_STEPS=%d]%n",
						FOST_ITERS, FOSB_ITERS, this.clusterCount, maxTimeSteps);

		for (int timeStep = 0; timeStep < maxTimeSteps; timeStep++) {

			long timeStepTime = System.currentTimeMillis();

			// PRINTOUT
			System.out.printf("\tFOS/T [TimeStep:%d, All Nodes]...", timeStep);

			Transaction tx = transNeo.beginTx();

			try {
				for (Node v : transNeo.getAllNodes()) {
					if (v.getId() != 0) {

						// For Every "Cluster System"
						for (int c = 0; c < clusterCount; c++) {
							// FOS/T Primary Diffusion Algorithm
							do_FOST(v, c);
						}

					}
				}

				tx.success();

			} catch (Exception ex) {
				System.err.printf("<ERR: DiDiC Outer Loop Aborted>");
			} finally {
				tx.finish();
			}

			// PRINTOUT
			System.out.printf("%dms%n", System.currentTimeMillis()
					- timeStepTime);

			update_cluster_allocation(timeStep);

			// TODO: perform insertions/deletions to Neo4j instance
			// adaptToGraphChanges()
			
			// FIXME: for debugging purposes only!
			if (timeStep % 50 == 0) {
				closeTransServices();
				DiDiCPartitioner.write_chaco_and_ptn(Integer.toString(timeStep), clusterCount);
				openTransServices();
			}
		}

		// PRINTOUT
		System.out.printf("DiDiC Complete - Time Taken: %dms%n", System
				.currentTimeMillis()
				- time);

		closeTransServices();
		System.out.println("*********DiDiC***********\n");
	}

	private void init_cluster_allocation() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Cluster-Allocation...");

		Random rand = new Random();

		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {

					node.setProperty("color", rand.nextInt(clusterCount));

				}
			}

			tx.success();

		} catch (Exception ex) {
			System.err.printf("<ERR: Cluster Allocation Aborted>");
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

			for (Node v : transNeo.getAllNodes()) {
				if (v.getId() != 0) {

					int vColor = (Integer) v.getProperty("color");
					String vName = (String) v.getProperty("name");

					ArrayList<Double> wV = new ArrayList<Double>();
					for (int i = 0; i < clusterCount; i++) {
						if (vColor == i) {
							wV.add(new Double(MY_CLUSTER_VAL));
							continue;
						}
						wV.add(new Double(0));
					}
					w.put(vName, wV);

					ArrayList<Double> lV = new ArrayList<Double>();
					for (int i = 0; i < clusterCount; i++) {
						if (vColor == i) {
							lV.add(new Double(MY_CLUSTER_VAL));
							continue;
						}
						lV.add(new Double(0));
					}
					l.put(vName, lV);

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

	private void update_cluster_allocation(int timeStep) {
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

				// Integer vNewColor = allocate_cluster_basic(wC.getValue());
				Integer vNewColor = allocate_cluster_intdeg(v, wC.getValue());

				v.setProperty("color", vNewColor);
			}

			tx.success();

		} catch (Exception ex) {
			System.err.printf("<ERR: update_cluster_allocation>");
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
	}

	// MUST call from inside Transaction
	private void do_FOST(Node v, int c) {

		// long time = System.currentTimeMillis();

		try {
			String vName = (String) v.getProperty("name");

			// // PRINTOUT
			// System.out.printf("FOS/T [Node:%s, Cluster:%d]...", vName, c);

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
			System.err.printf("<ERR: do_FOST [Cluster:%d]>", c);
		} finally {
			// PRINTOUT
			// System.out.printf("%dms%n", System.currentTimeMillis() - time);
		}

	}

	// MUST call from inside Transaction
	private void do_FOSB(Node v, int c) {
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
			System.err.printf("<ERR: do_FOSB [Cluster:%d]>", c);
		} finally {
		}

	}

	// MUST call from inside Transaction
	private double alpha_e(Node u, Node v) {
		// alpha_e = 1/max{deg(u),deg(v)};
		int uDeg = 0;
		int vDeg = 0;

		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {
			vDeg++;
		}

		for (Relationship e : u.getRelationships(Direction.OUTGOING)) {
			uDeg++;
		}

		int max = Math.max(uDeg, vDeg);

		if (max == 0)
			return 1.0;

		return 1.0 / (double) max;
	}

	// MUST call from inside Transaction
	private int weight_e(Relationship e) {
		if (e.hasProperty("weight"))
			return (Integer) e.getProperty("weight");
		else
			return 1;
	}

	// MUST call from inside Transaction
	private int benefit(Node v, int c) {
		int benefit = 1;

		int myC = (Integer) v.getProperty("color");
		if (myC == c) {
			benefit = B;
		}

		return benefit;
	}

	// Assign to cluster:
	// * Associated with highest load value
	private int allocate_cluster_basic(ArrayList<Double> wC) {
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

	// Assign to cluster:
	// * Associated with highest load value
	// AND
	// * Internal Degree of v is greater than zero
	private int allocate_cluster_intdeg(Node v, ArrayList<Double> wC) {
		// TODO: here now
		int maxC = 0;
		double maxW = 0.0;

		for (int c = 0; c < wC.size(); c++) {
			if ((wC.get(c) > maxW) && (int_deg_not_zero(v, c))) {
				maxW = wC.get(c);
				maxC = c;
			}
		}

		return maxC;
	}

	// MUST call from inside Transaction
	// v has at least 1 edge to its own cluster
	private boolean int_deg_not_zero(Node v, int c) {
		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

			Node u = e.getEndNode();
			int uColor = (Integer) u.getProperty("color");

			if (c == uColor)
				return true;

		}

		return false;
	}

	// MUST call from inside Transaction
	// Number of edges v has to its own cluster
	private int int_deg(Node v, int c) {
		int intDeg = 0;
		for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

			Node u = e.getEndNode();
			int uColor = (Integer) u.getProperty("color");

			if (c == uColor)
				intDeg++;

		}

		return intDeg;
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
