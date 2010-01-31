package didic_neo4j;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import graph_gen_utils.NeoFromFile;

public class DiDiCPartitioner {

	private static final int FOST_ITERS = 11;
	private static final int FOSB_ITERS = 11;
	private static final int B = 10;
	private static final int MY_CLUSTER_VAL = 100;

	// private static final int ALPHA_e = 1/max{deg(u),deg(v)};

	private HashMap<String,ArrayList<Double>> w = null;
	private HashMap<String,ArrayList<Double>> l = null;

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
		
		w = new HashMap<String,ArrayList<Double>>();
		l = new HashMap<String,ArrayList<Double>>();
	}

	public void do_DiDiC(int timeSteps) {
		openTransServices();

		init_cluster_allocation();

		init_load_vectors();
		
		for (int i = 0; i < timeSteps; i++) {
			
			// TODO: for each node, for fair "node load balancing" 
			for (int j = 0; j < clusterCount; j++) {
				
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
	
	private void do_FOST() {
		long time = System.currentTimeMillis();

		// PRINTOUT
		System.out.printf("FOS/T (%d Iterations)...",FOST_ITERS);

		Transaction tx = transNeo.beginTx();

		try {
			
			for (Node node : transNeo.getAllNodes()) {

				if (node.getId() != 0) {
				}
				
			}

		} catch (Exception ex) {
			System.err.printf("<ERR: do_FOST>");
		} finally {
			tx.finish();
		}

		// PRINTOUT
		System.out.printf("%dms%n", System.currentTimeMillis() - time);
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
