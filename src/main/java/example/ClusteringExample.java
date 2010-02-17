package example;

import graph_cluster_algorithms.ConfDiDiC;
import graph_cluster_algorithms.AlgDiDiC;
import graph_cluster_supervisor.Supervisor;
import graph_cluster_supervisor.SupervisorDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ClusterInitType;

import java.io.FileNotFoundException;

public class ClusteringExample {

	// Debugging Related
	private static final int SNAPSHOT_PERIOD = 5;
	private static final int MAX_ITERATIONS = 150;

	public static void main(String[] args) {
		// do_didic_add20_16_base_random_T11B11();
		// do_didic_add20_16_base_balanced_T11B11();
		// do_didic_add20_16_opt_random_T11B11();
		// do_didic_add20_16_opt_balanced_T11B11();
		// do_didic_test_2_base_balanced_T5B5();
		// do_didic_uk_2_base_random_T11B11();
		// do_didic_uk_2_base_balanced_T11B11();
		// do_didic_uk_16_base_random_T11B11();
		do_didic_uk_16_base_balanced_T11B11();
	}

	private static void do_didic_test_2_base_balanced_T5B5() {
		int clusterCount = 2;

		String inputGraph = "test-DiDiC";
		String inputPtn = "test-DiDiC-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/test-DiDiC 2 Base Balanced T5 B5/";

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

	private static void do_didic_add20_16_base_random_T11B11() {
		int clusterCount = 16;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-RAND";

		String databaseDir = String.format("var/%s-16-random", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/add20 16 Base Random/";

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

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_add20_16_base_balanced_T11B11() {
		int clusterCount = 16;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/add20 16 Base Balanced/";

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

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_add20_16_opt_random_T11B11() {
		int clusterCount = 16;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-RAND";

		String databaseDir = String.format("var/%s-16-random", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String resultsDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/add20 16 Opt Random/";

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
				inputGraph, graphDir, ptnDir, resultsDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_add20_16_opt_balanced_T11B11() {
		int clusterCount = 16;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String resultsDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/add20 16 Opt Balanced/";

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
				inputGraph, graphDir, ptnDir, resultsDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_uk_2_base_balanced_T11B11() {
		int clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/uk 2 Base Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeo(inputGraphPath, ClusterInitType.BALANCED,
					clusterCount);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		AlgDiDiC didic = new AlgDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_uk_2_base_random_T11B11() {
		int clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-RAND";

		String databaseDir = String.format("var/%s-2-random", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/uk 2 Base Random T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeo(inputGraphPath, ClusterInitType.RANDOM,
					clusterCount);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		AlgDiDiC didic = new AlgDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_uk_16_base_balanced_T11B11() {
		int clusterCount = 16;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/uk 16 Base Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeo(inputGraphPath, ClusterInitType.BALANCED,
					clusterCount);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		AlgDiDiC didic = new AlgDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_didic_uk_16_base_random_T11B11() {
		int clusterCount = 16;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-RAND";

		String databaseDir = String.format("var/%s-16-random", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "/home/alex/Dropbox/Neo_Thesis_Private/Results/uk 16 Base Random T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeo(inputGraphPath, ClusterInitType.RANDOM,
					clusterCount);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		AlgDiDiC didic = new AlgDiDiC();

		Supervisor didicSupervisor = new SupervisorDiDiC(SNAPSHOT_PERIOD,
				inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor);
	}

}
