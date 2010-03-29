package example;

import graph_cluster_algorithms.AlgDiskEvoPartition;
import graph_cluster_algorithms.AlgDiskEvoPartitionExp;
import graph_cluster_algorithms.AlgMemDiDiC;
import graph_cluster_algorithms.AlgMemDiDiCExpBal;
import graph_cluster_algorithms.AlgDiskDiDiC;
import graph_cluster_algorithms.AlgMemDiDiCExpFix;
import graph_cluster_algorithms.AlgMemDiDiCExpPaper;
import graph_cluster_algorithms.AlgMemDiDiCExpSync;
import graph_cluster_algorithms.AlgParaMemDiDiC;
import graph_cluster_algorithms.configs.ConfDiDiC;
import graph_cluster_algorithms.configs.ConfEvoPartition;
import graph_cluster_algorithms.supervisors.Supervisor;
import graph_cluster_algorithms.supervisors.SupervisorBase;
import graph_cluster_algorithms.supervisors.SupervisorPara;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ClusterInitType;
import graph_gen_utils.gml.GMLWriterUndirected;
import graph_gen_utils.graph.MemGraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Stack;

import pGraphService.PGraphDatabaseService;
import pGraphService.PGraphDatabaseServiceImpl;

public class ClusteringExample {

	// Debugging Related
	private static final int SNAPSHOT_PERIOD = 5;
	private static final int LONG_SNAPSHOT_PERIOD = 50;
	private static final int MAX_ITERATIONS = 150;

	public static void main(String[] args) throws Exception {
		// *****************
		// *** ESP Tests ***
		// *****************
		// do_esp_test_single();

		// **************************************
		// *** ESP Experimental Version Tests ***
		// **************************************
		// do_esp_exp_forest_fire_single();
		// do_esp_exp_uk_single();

		// ***************************
		// *** DiDiC On-Disk Tests ***
		// ***************************
		// do_disk_didic_test_2_opt_balanced_T11B11();

		// *****************************
		// *** DiDiC In-Memory Tests ***
		// *****************************
		// do_mem_didic_test_2_opt_balanced_T11B11();

		// do_mem_didic_uk_2_opt_balanced_T11B11();
		// do_mem_didic_add20_2_opt_balanced_T11B11();
		// do_mem_didic_m14b_2_opt_balanced_T11B11();

		// do_mem_didic_uk_16_opt_balanced_T11B11();
		// do_mem_didic_add20_16_opt_balanced_T11B11();

		// ****************************************************************
		// *** DiDiC In-Memory Experimental Balanced Cluster Size Tests ***
		// ****************************************************************
		// do_mem_didic_exp_bal_test_2_opt_balanced_T11B11();

		// do_mem_didic_exp_bal_uk_2_opt_balanced_T11B11();

		// do_mem_didic_exp_bal_add20_16_opt_balanced_T11B11();
		// do_mem_didic_exp_bal_uk_16_opt_balanced_T11B11();

		// ****************************************
		// *** DiDiC In-Memory Experimental Fix ***
		// ****************************************
		// do_mem_didic_exp_fix_uk_2_opt_balanced_T11B11();

		// ****************************************
		// *** DiDiC In-Memory Experimental Sync ***
		// ****************************************
		// do_mem_didic_exp_sync_uk_2_opt_balanced_T11B11();

		// **************************************************
		// *** DiDiC In-Memory Experimental Updated Paper ***
		// **************************************************
		// do_mem_didic_exp_paper_uk_2_opt_balanced_T11B11();

		// *******************************************************************
		// *** DiDiC In-Memory Using The Partitioned PGraphDatabaseService ***
		// *******************************************************************
		do_para_mem_didic_test_2_opt_balanced_T11B11();

	}

	private static void do_disk_didic_test_2_opt_balanced_T11B11()
			throws FileNotFoundException {
		byte clusterCount = 2;

		String inputGraph = "test-cluster";
		String inputPtn = "test-cluster-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/DiDiC - test-cluster 2 Base Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		AlgDiskDiDiC didic = new AlgDiskDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor);
	}

	private static void do_mem_didic_test_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

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

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_exp_bal_test_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "test-cluster";
		String inputPtn = "test-cluster-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Balanced - test-cluster 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(16);
		config.setClusterSizeOn(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_exp_bal_add20_16_opt_balanced_T11B11()
			throws FileNotFoundException {
		byte clusterCount = 16;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Balanced - add20 16 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(250);
		config.setClusterSizeOn(200);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_add20_16_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 16;

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

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_add20_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "add20";
		String inputPtn = "add20-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - add20 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

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

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, ClusterInitType.SINGLE,
				(byte) -1);

		AlgDiskEvoPartition esp = new AlgDiskEvoPartition();

		Supervisor espSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.start(databaseDir, config, espSupervisor);
	}

	private static void do_esp_exp_forest_fire_single() throws Exception {
		String inputGraph = "forest-fire-100";

		String databaseDir = String.format("var/%s", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/ESP Exp - forest-fire-100 Single/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);

		cleanDir(databaseDir);
		cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, ClusterInitType.SINGLE,
				(byte) -1);
		// neoGenerator
		// .writeNeoFromGML("/home/alex/workspace/graph_cluster_utils/graphs/esp.gml");

		AlgDiskEvoPartitionExp esp = new AlgDiskEvoPartitionExp();

		Supervisor espSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.start(databaseDir, config, espSupervisor);
	}

	private static void do_esp_exp_uk_single() throws Exception {
		byte clusterCount = 16;
		String inputGraph = "add20";

		String databaseDir = String.format("var/%s", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/ESP Exp - add20 Single/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);

		cleanDir(databaseDir);
		cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, ClusterInitType.SINGLE,
				(byte) -1);

		AlgDiskEvoPartitionExp esp = new AlgDiskEvoPartitionExp();

		Supervisor espSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);
		config.setClusterCount((long) 128);

		esp.start(databaseDir, config, espSupervisor);
	}

	private static void do_mem_didic_exp_bal_uk_16_opt_balanced_T11B11()
			throws FileNotFoundException {
		byte clusterCount = 16;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Balanced - uk 16 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(500);
		config.setClusterSizeOn(400);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_exp_bal_uk_2_opt_balanced_T11B11()
			throws FileNotFoundException {
		byte clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Balanced - uk 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(3200);
		config.setClusterSizeOn(3000);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_uk_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - uk 2 Opt Balanced T11 B11 - New Test/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_uk_16_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 16;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-16-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - uk 16 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_m14b_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "m14b";
		// String inputPtn = "m14b-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC - m14b 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		// String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
		// clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		// neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiC didic = new AlgMemDiDiC();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_exp_fix_uk_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Exp Fix - uk 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpFix didic = new AlgMemDiDiCExpFix();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_exp_sync_uk_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Exp Sync - uk 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpSync didic = new AlgMemDiDiCExpSync();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_mem_didic_exp_paper_uk_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "uk";
		String inputPtn = "uk-IN-BAL";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Exp Paper - uk 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraph();

		AlgMemDiDiCExpPaper didic = new AlgMemDiDiCExpPaper();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(databaseDir, config, didicSupervisor, memGraph);
	}

	private static void do_para_mem_didic_test_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "test-cluster";
		String inputPtn = "test-cluster-IN-RAND";

		String singleDatabaseDir = String.format("var/%s-2-random", inputGraph);
		String paraDatabaseDir = String.format("var/para-%s-2-random",
				inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Para Mem DiDiC - test 2 Opt Random T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		cleanDir(singleDatabaseDir);
		cleanDir(paraDatabaseDir);
		cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(singleDatabaseDir);

		// neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);
		neoGenerator.writeNeoFromChaco(inputGraphPath, ClusterInitType.BALANCED,
				(byte) 2);

		MemGraph memGraph = neoGenerator.readMemGraph();

		PGraphDatabaseService paraNeo = new PGraphDatabaseServiceImpl(
				paraDatabaseDir, 1);
		paraNeo.createDistribution(singleDatabaseDir);
		paraNeo.shutdown();

		AlgParaMemDiDiC didic = new AlgParaMemDiDiC();

		Supervisor didicSupervisor = new SupervisorPara(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(150);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(paraDatabaseDir, config, didicSupervisor, memGraph);
	}

	public static void cleanDir(String path) {
		deleteDir(path);
		File dir = new File(path);
		dir.mkdir();
	}

	public static void deleteDir(String path) {
		File dir = new File(path);

		if (dir.exists() == false)
			return;

		Stack<File> dirStack = new Stack<File>();
		dirStack.push(dir);

		boolean containsSubFolder;
		while (!dirStack.isEmpty()) {
			File currDir = dirStack.peek();
			containsSubFolder = false;

			String[] fileArray = currDir.list();
			for (int i = 0; i < fileArray.length; i++) {
				String fileName = currDir.getAbsolutePath() + File.separator
						+ fileArray[i];
				File file = new File(fileName);
				if (file.isDirectory()) {
					dirStack.push(file);
					containsSubFolder = true;
				} else {
					file.delete(); // delete file
				}
			}

			if (!containsSubFolder) {
				dirStack.pop(); // remove curr dir from stack
				currDir.delete(); // delete curr dir
			}
		}

	}

}
