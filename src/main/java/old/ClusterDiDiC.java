package old;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class ClusterDiDiC {

	// Debugging Related
	private static final String IN_GRAPH = "add20";
	private static final String IN_PTN = "add20-IN-RAND";
	// private static final String IN_GRAPH = "test-DiDiC";
	// private static final String IN_PTN = "test-DiDiC-BAL";
	private static final int DUMP_PERIOD = 5;
	private static final int CLUSTER_COUNT = 2;

	// DiDiC Related
	private static final int FOST_ITERS = 11; // Primary Diffusion
	private static final int FOSB_ITERS = 11; // Secondary Diffusion ('drain')
	private static final int B_LOW = 1; // Benefit, used by FOS/B
	private static final int B_HIGH = 10; // Benefit, used by FOS/B
	private static final int MY_CLUSTER_VAL = 100; // Default Init Val

	// Experimental DiDiC Related
	private static final int ALG_SWITCH_POINT = -1;

	private HashMap<String, ArrayList<Double>> w = null; // Load Vec 1
	private HashMap<String, ArrayList<Double>> l = null; // Load Vec 2 ('drain')

	private int clusterCount;
	private String databaseDir;

	private GraphDatabaseService transNeo = null;
	private IndexService transIndexService = null;

	public static void main(String[] args) {
		test_DiDiC_no_init();
		// test_DiDiC_init();
	}

	private static void test_DiDiC_no_init() {
		try {

			String inNeo = String.format("var/%s", IN_GRAPH);
			String inGraph = String.format("graphs/%s.graph", IN_GRAPH);
			String inPtn = String.format("partitionings/%s.%s.ptn", IN_PTN,
					CLUSTER_COUNT);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(inNeo);

			// To generate coloured/partitioned neo4j graph
			// * Assign input Chaco graph file & input partitioning file
			neoCreator.generateNeo(inGraph, inPtn);

			ClusterDiDiC didic = new ClusterDiDiC(CLUSTER_COUNT, inNeo);
			didic.do_DiDiC(150, false);

			snapshot_chaco_and_ptn("FINAL", CLUSTER_COUNT);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void test_DiDiC_init() {
		try {

			String inNeo = String.format("var/%s", IN_GRAPH);
			String inGraph = String.format("graphs/%s.graph", IN_GRAPH);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(inNeo);

			// To generate coloured/partitioned neo4j graph
			// * Assign input Chaco graph file & input partitioning file
			neoCreator.generateNeo(inGraph);

			ClusterDiDiC didic = new ClusterDiDiC(CLUSTER_COUNT, inNeo);
			didic.do_DiDiC(150, true);

			snapshot_chaco_and_ptn("FINAL", CLUSTER_COUNT);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void snapshot_chaco_and_ptn(String version, int clusterCount) {
		try {
			String clusterCountStr = Integer.toString(clusterCount);
			String inNeo = String.format("var/%s", IN_GRAPH);

			String outGraph = String.format("graphs/%s-%s.graph", IN_GRAPH,
					version);
			String outPtn = String.format("partitionings/%s-%s.%s.ptn",
					IN_GRAPH, version, clusterCountStr);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreatorIn = new NeoFromFile(inNeo);

			neoCreatorIn.generateChaco(outGraph,
					NeoFromFile.ChacoType.UNWEIGHTED, outPtn);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void snapshot_metrics(String version, int clusterCount) {
		try {

			String clusterCountStr = Integer.toString(clusterCount);
			String inNeo = String.format("var/%s", IN_GRAPH);
			String outMetrics = String.format("metrics/%s-%s.%s.met", IN_GRAPH,
					version, clusterCountStr);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(inNeo);

			neoCreator.generateMetrics(outMetrics);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ClusterDiDiC(int clusterCount, String databaseDir) {
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

		if (initClusters) {
			// init_cluster_alloc_random();
			init_cluster_alloc_balanced();
		}

		closeTransServices();
		ClusterDiDiC.snapshot_metrics("INIT", clusterCount);
		openTransServices();

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
				// For Every "Cluster System"
				for (int c = 0; c < clusterCount; c++) {

					// For Every Node
					for (Node v : transNeo.getAllNodes()) {
						if (v.getId() != 0) {

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

			// FIXME: For debugging purposes only! Remove later!
			if (timeStep % DUMP_PERIOD == 0) {
				closeTransServices();
				ClusterDiDiC.snapshot_metrics(Integer.toString(timeStep),
						clusterCount);
				openTransServices();
			}
		}

		// FIXME: For debugging purposes only! Remove later!
		closeTransServices();
		ClusterDiDiC.snapshot_metrics("FINAL", clusterCount);
		openTransServices();

		// PRINTOUT
		long ms_total = System.currentTimeMillis() - time;
		long ms = ms_total % 1000;
		long s = (ms_total / 1000) % 60;
		long m = (ms_total / 1000) / 60;
		System.out.printf("DiDiC Complete - Time Taken: %d(m):%d(s):%d(ms)%n",
				m, s, ms);

		closeTransServices();
		System.out.println("*********DiDiC***********\n");
	}

	private void init_cluster_alloc_random() {
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

	private void init_cluster_alloc_balanced() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("Initialising Cluster-Allocation...");

		Integer c = 0;
		Transaction tx = transNeo.beginTx();

		try {
			for (Node node : transNeo.getAllNodes()) {
				if (node.getId() != 0) {

					node.setProperty("color", c++);

					if (c >= clusterCount)
						c = 0;

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
					ArrayList<Double> lV = new ArrayList<Double>();

					for (int i = 0; i < clusterCount; i++) {

						if (vColor == i) {
							wV.add(new Double(MY_CLUSTER_VAL));
							lV.add(new Double(MY_CLUSTER_VAL));
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

				// Integer vNewColor = allocate_cluster(v, wC.getValue(),
				// timeStep);
				// Integer vNewColor = allocate_cluster_basic(wC.getValue());
				Integer vNewColor = allocate_cluster_intdeg(v, wC.getValue(),
						timeStep);

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

			ArrayList<Double> lV = l.get(vName);
			ArrayList<Double> wV = w.get(vName);

			double wVC = wV.get(c);

			int vDeg = in_deg(v);

			for (int fost_iter = 0; fost_iter < FOST_ITERS; fost_iter++) {

				// FOS/B (Secondary/Drain) Diffusion Algorithm
				do_FOSB(v, c);

				// FOS/T Primary Diffusion Algorithm
				for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

					Node u = e.getEndNode();
					ArrayList<Double> wU = w.get(u.getProperty("name"));

					double wVwUDiff = wVC - wU.get(c);

					wVC = wVC - alpha_e(u, vDeg) * weight_e(e) * wVwUDiff;
				}

				wVC = wVC + lV.get(c);

				wV.set(c, wVC);
			}

		} catch (Exception ex) {
			System.err.printf("<ERR: do_FOST [Cluster:%d]>", c);
		}

	}

	// MUST call from inside Transaction
	private void do_FOSB(Node v, int c) {
		try {

			String vName = (String) v.getProperty("name");

			ArrayList<Double> lV = l.get(vName);

			double lVC = lV.get(c);
			int vDeg = in_deg(v);
			double bV = benefit(v, c);

			for (int fosb_iter = 0; fosb_iter < FOSB_ITERS; fosb_iter++) {

				// FOS/B Diffusion Algorithm
				for (Relationship e : v.getRelationships(Direction.OUTGOING)) {

					Node u = e.getEndNode();
					ArrayList<Double> lU = l.get(u.getProperty("name"));

					double lVlUDiff = (lVC / bV) - (lU.get(c) / benefit(u, c));

					lVC = lVC - alpha_e(u, vDeg) * weight_e(e) * lVlUDiff;
				}

			}

			lV.set(c, lVC);

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

		return 1.0 / (double) max;
	}

	// MUST call from inside Transaction
	// Optimized version. Find vDeg once only
	private double alpha_e(Node u, int vDeg) {
		// alpha_e = 1/max{deg(u),deg(v)};
		int uDeg = 0;

		for (Relationship e : u.getRelationships(Direction.OUTGOING)) {
			uDeg++;
		}

		double max = Math.max(uDeg, vDeg);

		return 1.0 / max;
	}

	// MUST call from inside Transaction
	private double weight_e(Relationship e) {
		if (e.hasProperty("weight"))
			return (Double) e.getProperty("weight");
		else
			return 1.0;
	}

	// MUST call from inside Transaction
	private double benefit(Node v, int c) {

		if ((Integer) v.getProperty("color") == c)
			return B_HIGH;
		else
			return B_LOW;
	}

	// MUST call from inside Transaction
	// Switch between algorithms depending on time-step
	private int allocate_cluster(Node v, ArrayList<Double> wC, int timeStep) {
		// Choose cluster with largest load vector
		if ((timeStep < ALG_SWITCH_POINT) || (ALG_SWITCH_POINT == -1))
			return allocate_cluster_basic(wC);

		// Optimization to exclude clusters with no connections
		else
			return allocate_cluster_intdeg(v, wC, timeStep);
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

	// Optimization to exclude clusters with no connections
	// Assign to cluster:
	// * Associated with highest load value
	// AND
	// * Internal Degree of v is greater than zero
	private int allocate_cluster_intdeg(Node v, ArrayList<Double> wC,
			int timeStep) {

		int maxC = (Integer) v.getProperty("color");
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
	// v has at least 1 edge to given cluster
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
	private int in_deg(Node v) {
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

}
