package example;

import graph_cluster_algorithms.AlgNibbleESP;
import graph_cluster_algorithms.ConfDiDiC;
import graph_cluster_algorithms.AlgDiDiC;
import graph_cluster_algorithms.ConfNibbleESP;
import graph_cluster_supervisor.Supervisor;
import graph_cluster_supervisor.SupervisorDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ClusterInitType;

import java.io.FileNotFoundException;

import javax.naming.ConfigurationException;

public class ClusteringExample {

	// Debugging Related
	private static final int SNAPSHOT_PERIOD = 5;
	private static final int MAX_ITERATIONS = 150;

	public static void main(String[] args) throws Exception {
//		do_didic_test_2_base_balanced_T5B5();
		do_esp_test_single();
	}

	private static void do_didic_test_2_base_balanced_T5B5() {
		int clusterCount = 2;

		String inputGraph = "test-cluster";
		String inputPtn = "test-cluster-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/DiDiC - test-cluster 2 Base Balanced T5 B5/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeo(inputGraphPath, inputPtnPath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		AlgDiDiC didic = new AlgDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(5);
		config.setFOSBIterations(5);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_esp_test_single() throws Exception {
		String inputGraph = "test-cluster";
		// String inputPtn = "test-cluster-IN-BAL";

		String databaseDir = String.format("var/%s", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/ESP - test-cluster 2 Single/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		// String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
		// 1);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			// neoGenerator.writeNeo(inputGraphPath, inputPtnPath);
			neoGenerator.writeNeo(inputGraphPath, ClusterInitType.SINGLE, -1);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		AlgNibbleESP esp = new AlgNibbleESP();

		Supervisor espSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfNibbleESP config = new ConfNibbleESP();
		config.setP(0.5);
		config.setTheta(0.01);
		
		esp.start(databaseDir, config, espSupervisor);
	}

}
