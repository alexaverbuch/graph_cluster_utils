package example;

import graph_cluster_algorithms.AlgEvoPartition;
import graph_cluster_algorithms.AlgMemDiDiC;
import graph_cluster_algorithms.ConfDiDiC;
import graph_cluster_algorithms.AlgDiDiC;
import graph_cluster_algorithms.ConfEvoPartition;
import graph_cluster_supervisor.Supervisor;
import graph_cluster_supervisor.SupervisorDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ClusterInitType;
import graph_gen_utils.graph.MemGraph;

import java.io.FileNotFoundException;

import javax.naming.ConfigurationException;

public class ClusteringExample {

	// Debugging Related
	private static final int SNAPSHOT_PERIOD = 5;
	private static final int MAX_ITERATIONS = 150;

	public static void main(String[] args) throws Exception {
		// do_didic_test_2_base_balanced_T5B5();
		// do_esp_test_single();
		// do_mem_didic_test_2_base_balanced_T5B5();
		// do_mem_add20_16_opt_balanced_T11B11();
		do_mem_didic_test_2_opt_balanced_T11B11();
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

	private static void do_mem_didic_test_2_base_balanced_T5B5() {
		int clusterCount = 2;

		String inputGraph = "test-cluster";
		String inputPtn = "test-cluster-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - test-cluster 2 Base Balanced T5 B5/";

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

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(5);
		config.setFOSBIterations(5);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_test_2_opt_balanced_T11B11() {
		int clusterCount = 2;

		String inputGraph = "test-cluster";
		String inputPtn = "test-cluster-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - test-cluster 2 Opt Balanced T11 B11/";

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

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_add20_16_opt_balanced_T11B11() {
		int clusterCount = 16;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - add20 16 Opt Balanced T11 B11/";

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

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
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

		AlgEvoPartition esp = new AlgEvoPartition();

		Supervisor espSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.start(databaseDir, config, espSupervisor);
	}

}
