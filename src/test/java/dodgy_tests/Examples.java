package dodgy_tests;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.logger.LoggerBase;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.migrator.MigratorBase;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCBal;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCBase;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCFix;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCPaper;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCSync;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_cluster_utils.ptn_alg.esp.PtnAlgEvoPartitionBase;
import graph_cluster_utils.ptn_alg.esp.PtnAlgEvoPartitionExp;
import graph_cluster_utils.ptn_alg.esp.config.ConfEvoPartition;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.NeoFromFile.ChacoType;
import graph_gen_utils.general.DirUtils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.memory_graph.MemNode;
import graph_gen_utils.memory_graph.MemRel;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsRandom;
import graph_gen_utils.partitioner.PartitionerAsSingle;

import java.io.FileNotFoundException;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.core.PGraphDatabaseServiceImpl;

public class Examples {

	// Debugging Related
	private static final int SNAPSHOT_PERIOD = 5;
	private static final int LONG_SNAPSHOT_PERIOD = 5;
	private static final int MAX_ITERATIONS = 150;

	public static void doExamples() {

		try {

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
			// do_mem_didic_exp_paper_test0_2_opt_balanced_T11B11();

			// *******************************************************************
			// *** DiDiC In-Memory Using The Partitioned PGraphDatabaseService
			// ***
			// *******************************************************************
			// do_para_mem_didic_test_2_opt_balanced_T11B11();

		} catch (Exception e) {
			e.printStackTrace();
		}

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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(transNeo, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBal(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(16);
		config.setClusterSizeOn(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBal(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(250);
		config.setClusterSizeOn(200);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				partitioner);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg esp = new PtnAlgEvoPartitionBase(transNeo, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.doPartition(config);
		transNeo.shutdown();
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

		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				partitioner);
		// NeoFromFile
		// .writeNeoFromGML(transNeo,"/home/alex/workspace/graph_cluster_utils/graphs/esp.gml");

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg esp = new PtnAlgEvoPartitionExp(transNeo, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);

		esp.doPartition(config);
		transNeo.shutdown();
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

		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		Partitioner partitioner = new PartitionerAsSingle((byte) -1);
		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				partitioner);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg esp = new PtnAlgEvoPartitionExp(transNeo, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfEvoPartition config = new ConfEvoPartition();
		config.setP(0.9);
		config.setTheta(0.01);
		config.setConductance(0.001);
		config.setClusterCount((long) 128);

		esp.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBal(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(500);
		config.setClusterSizeOn(400);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBal(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);
		config.setClusterSizeOff(3200);
		config.setClusterSizeOn(3000);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		// NeoFromFile.writeNeoFromChaco(transNeo, inputGraphPath,
		// inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(MAX_ITERATIONS);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCFix(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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
		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCSync(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
	}

	private static void do_mem_didic_exp_sync_romania_2_opt_balanced_T11B11()
			throws Exception {

		byte clusterCount = 2;

		String databaseDir = "var/romania-balanced2-named-coords_all";

		String metDir = "metrics/Mem DiDiC Exp Sync - romania 2 Opt Balanced T11 B11/";

		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);
		// Partitioner partitioner = new PartitionerAsBalanced((byte) 2);
		// NeoFromFile.applyPtnToNeo(transNeo, partitioner);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(1, 5, "romania", metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCSync(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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

		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCPaper(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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

		GraphDatabaseService transNeo = new EmbeddedGraphDatabase(databaseDir);

		NeoFromFile.writeNeoFromChacoAndPtn(transNeo, inputGraphPath,
				inputPtnPath);

		MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCPaper(memGraph, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(500);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeo.shutdown();
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

		GraphDatabaseService transNeoSingle = new EmbeddedGraphDatabase(
				singleDatabaseDir);

		Partitioner partitioner = new PartitionerAsRandom((byte) 2);
		NeoFromFile.writeNeoFromChacoAndPtn(transNeoSingle, inputGraphPath,
				partitioner);

		// MemGraph memGraph = NeoFromFile.readMemGraph(transNeoSingle);
		transNeoSingle.shutdown();

		PGraphDatabaseService transNeoPart = NeoFromFile.writePNeoFromNeo(
				paraDatabaseDir, transNeoSingle);

		Logger logger = new LoggerBase(SNAPSHOT_PERIOD, LONG_SNAPSHOT_PERIOD,
				inputGraph, metDir);

		Migrator migrator = new MigratorBase(null, Integer.MAX_VALUE);

		PtnAlg didic = new PtnAlgDiDiCBase(transNeoPart, logger,
				new LinkedBlockingQueue<ChangeOp>(), migrator);

		ConfDiDiC config = new ConfDiDiC(clusterCount);
		config.setAllocType(ConfDiDiC.AllocType.OPT);
		config.setMaxIterations(150);
		config.setFOSTIterations(11);
		config.setFOSBIterations(11);

		didic.doPartition(config);
		transNeoPart.shutdown();
	}

}
