package example;

import graph_cluster_algorithms.ClusterAlgDiDiC;
import graph_cluster_utils.Supervisor;
import graph_cluster_utils.SupervisorDiDiC;
import graph_gen_utils.NeoFromFile;

import java.io.FileNotFoundException;

public class ClusteringExample {

	// Debugging Related
	private static final int CLUSTER_COUNT = 2;
	private static final int SNAPSHOT_PERIOD = 5;
	private static final int MAX_ITERATIONS = 150;

	public static void main(String[] args) {
//		String inputGraph = "add20";
//		String inputPtn = "add20-IN-RAND";
		String inputGraph = "test-DiDiC";
		String inputPtn = "test-DiDiC-IN-BAL";

		String databaseDir = String.format("var/%s", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%s.ptn", ptnDir, inputPtn,
				CLUSTER_COUNT);
		
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.generateNeo(inputGraphPath, inputPtnPath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		ClusterAlgDiDiC didic = new ClusterAlgDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		didic.do_DiDiC(databaseDir, MAX_ITERATIONS, CLUSTER_COUNT,
				ClusterAlgDiDiC.InitType.DEFAULT, didicSupervisor);

	}
}
