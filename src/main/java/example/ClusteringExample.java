package example;

import graph_cluster_utils.alg.config.ConfDiDiC;
import graph_cluster_utils.alg.config.ConfEvoPartition;
import graph_cluster_utils.alg.disk.AlgDisk;
import graph_cluster_utils.alg.disk.didic.AlgDiskDiDiC;
import graph_cluster_utils.alg.disk.esp.AlgDiskEvoPartition;
import graph_cluster_utils.alg.disk.esp.AlgDiskEvoPartitionExp;
import graph_cluster_utils.alg.mem.didic.AlgMemDiDiC;
import graph_cluster_utils.alg.mem.didic.AlgMemDiDiCExpBal;
import graph_cluster_utils.alg.mem.didic.AlgMemDiDiCExpFix;
import graph_cluster_utils.alg.mem.didic.AlgMemDiDiCExpPaper;
import graph_cluster_utils.alg.mem.didic.AlgMemDiDiCExpSync;
import graph_cluster_utils.alg.mem.didic.AlgMemPartDiDiC;
import graph_cluster_utils.supervisor.Supervisor;
import graph_cluster_utils.supervisor.SupervisorBase;
import graph_cluster_utils.supervisor.SupervisorPart;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;
import graph_gen_utils.general.DirUtils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;
import graph_gen_utils.memory_graph.MemRel;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsBalanced;
import graph_gen_utils.partitioner.PartitionerAsRandom;
import graph_gen_utils.partitioner.PartitionerAsSingle;

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
		do_mem_didic_exp_sync_romania_2_opt_balanced_T11B11();

		// **************************************************
		// *** DiDiC In-Memory Experimental Updated Paper ***
		// **************************************************
		// do_mem_didic_exp_paper_uk_2_opt_balanced_T11B11();
		// do_mem_didic_exp_paper_imdb_2_base_random_T11B11();
		// do_mem_didic_exp_paper_test0_2_opt_balanced_T11B11();

		// *******************************************************************
		// *** DiDiC In-Memory Using The Partitioned PGraphDatabaseService ***
		// *******************************************************************
		// do_para_mem_didic_test_2_opt_balanced_T11B11();

	}

	private static void do_disk_didic_test_2_opt_balanced_T11B11()
			throws Exception {
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgDisk didic = new AlgDiskDiDiC(databaseDir, didicSupervisor);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiC didic = new AlgMemDiDiC(databaseDir, didicSupervisor,
				memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(16);
		config.setClusterSizeOn(11);

		didic.start(config);
	}

	private static void do_mem_didic_exp_bal_add20_16_opt_balanced_T11B11()
			throws Exception {
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(250);
		config.setClusterSizeOn(200);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		try {
			neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiC didic = new AlgMemDiDiC(databaseDir, didicSupervisor,
				memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiC didic = new AlgMemDiDiC(databaseDir, didicSupervisor,
				memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, partitioner);

		Supervisor espSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgDiskEvoPartition esp = new AlgDiskEvoPartition(databaseDir,
				espSupervisor);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.start(config);
	}

	private static void do_esp_exp_forest_fire_single() throws Exception {
		String inputGraph = "forest-fire-100";

		String databaseDir = String.format("var/%s", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/ESP Exp - forest-fire-100 Single/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);

		DirUtils.cleanDir(databaseDir);
		DirUtils.cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, partitioner);
		// neoGenerator
		// .writeNeoFromGML("/home/alex/workspace/graph_cluster_utils/graphs/esp.gml");

		Supervisor espSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgDiskEvoPartitionExp esp = new AlgDiskEvoPartitionExp(databaseDir,
				espSupervisor);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.start(config);
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

		DirUtils.cleanDir(databaseDir);
		DirUtils.cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, partitioner);

		Supervisor espSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgDiskEvoPartitionExp esp = new AlgDiskEvoPartitionExp(databaseDir,
				espSupervisor);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);
		config.setClusterCount((long) 128);

		esp.start(config);
	}

	private static void do_mem_didic_exp_bal_uk_16_opt_balanced_T11B11()
			throws Exception {
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(500);
		config.setClusterSizeOn(400);

		didic.start(config);
	}

	private static void do_mem_didic_exp_bal_uk_2_opt_balanced_T11B11()
			throws Exception {
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpBal didic = new AlgMemDiDiCExpBal(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(3200);
		config.setClusterSizeOn(3000);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiC didic = new AlgMemDiDiC(databaseDir, didicSupervisor,
				memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiC didic = new AlgMemDiDiC(databaseDir, didicSupervisor,
				memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		// neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiC didic = new AlgMemDiDiC(databaseDir, didicSupervisor,
				memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpFix didic = new AlgMemDiDiCExpFix(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpSync didic = new AlgMemDiDiCExpSync(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
	}

	private static void do_mem_didic_exp_sync_romania_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String databaseDir = String.format("var/%s-2-balanced", "romania");

		String metDir = "metrics/Mem DiDiC Exp Sync - romania 2 Opt Balanced T11 B11/";

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);
		Partitioner partitioner = new PartitionerAsBalanced((byte) 2);
		neoGenerator.applyPtnToNeo(partitioner);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, "romania", metDir);

		AlgMemDiDiCExpSync didic = new AlgMemDiDiCExpSync(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(databaseDir);
		DirUtils.cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpPaper didic = new AlgMemDiDiCExpPaper(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
	}

	private static void do_mem_didic_exp_paper_test0_2_opt_balanced_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "test0";
		String inputPtn = "test0-IN";

		String databaseDir = String.format("var/%s-2-balanced", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Exp Paper - test0 2 Opt Balanced T11 B11/";

		String inputGraphPath = String.format("%s%s.graph", graphDir,
				inputGraph);
		String inputPtnPath = String.format("%s%s.%d.ptn", ptnDir, inputPtn,
				clusterCount);

		DirUtils.cleanDir(databaseDir);
		DirUtils.cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, inputPtnPath);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemDiDiCExpPaper didic = new AlgMemDiDiCExpPaper(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
	}

	private static void do_mem_didic_exp_paper_imdb_2_base_random_T11B11()
			throws Exception {
		byte clusterCount = 2;

		String inputGraph = "imdb";
		String inputPtn = "imdb-IN-BAL";

		String databaseDir = String.format("var/%s-2-random", inputGraph);

		String graphDir = "graphs/";
		String ptnDir = "partitionings/";
		String metDir = "metrics/Mem DiDiC Exp Paper - imdb 2 Opt Random T11 B11/";

		DirUtils.cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(databaseDir);

		Partitioner partitioner = new PartitionerAsRandom((byte) 2);
		neoGenerator.applyPtnToNeo(partitioner);

		neoGenerator.writeChacoAndPtn(String.format("%s%s.graph", metDir,
				inputGraph), ChacoType.UNWEIGHTED, String.format("%s%s.2.ptn",
				metDir, inputPtn));

		neoGenerator.writeGMLBasic(String.format("%s%s-2-random.INIT.gml",
				metDir, inputGraph));

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		Supervisor didicSupervisor = new SupervisorBase(5, 10, inputGraph,
				metDir);

		AlgMemDiDiCExpPaper didic = new AlgMemDiDiCExpPaper(databaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.BASE);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
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

		DirUtils.cleanDir(singleDatabaseDir);
		DirUtils.cleanDir(paraDatabaseDir);
		DirUtils.cleanDir(metDir);

		NeoFromFile neoGenerator = new NeoFromFile(singleDatabaseDir);

		// neoGenerator.writeNeoFromChaco(inputGraphPath, inputPtnPath);
		Partitioner partitioner = new PartitionerAsRandom((byte) 2);
		neoGenerator.writeNeoFromChacoAndPtn(inputGraphPath, partitioner);

		MemGraph memGraph = neoGenerator.readMemGraphUndirected();

		PGraphDatabaseService paraNeo = new PGraphDatabaseServiceImpl(
				paraDatabaseDir, 1);
		paraNeo.createDistribution(singleDatabaseDir);
		paraNeo.shutdown();

		Supervisor didicSupervisor = new SupervisorPart(SNAPSHOT_PERIOD,
				LONG_SNAPSHOT_PERIOD, inputGraph, metDir);

		AlgMemPartDiDiC didic = new AlgMemPartDiDiC(paraDatabaseDir,
				didicSupervisor, memGraph);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(150);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.start(config);
	}

	private void checkMemGraphSymmetry(MemGraph memGraph) throws Exception {
		for (MemNode memV : memGraph.getAllNodes()) {
			for (MemRel memE : memV.getNeighbours()) {
				if (memGraph.getNode(memE.getEndNodeId()).hasNeighbour(
						memV.getId()) == false)

					throw new Exception(String.format(
							"Not Symmetrical. NodeId %d%n", memV.getId()));

			}
		}
	}

}
