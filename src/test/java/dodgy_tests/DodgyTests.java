package dodgy_tests;

import graph_cluster_utils.alg_runner.AlgRunner;
import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.change_log.ChangeOpAddNode;
import graph_cluster_utils.change_log.ChangeOpAddRelationship;
import graph_cluster_utils.change_log.ChangeOpDeleteNode;
import graph_cluster_utils.change_log.ChangeOpDeleteRelationship;
import graph_cluster_utils.change_log.ChangeOpEnd;
import graph_cluster_utils.change_log.LogReaderChangeOp;
import graph_cluster_utils.config.Conf;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.logger.LoggerBase;
import graph_cluster_utils.logger.LoggerMetricsMinimal;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.migrator.MigratorBase;
import graph_cluster_utils.migrator.MigratorDummy;
import graph_cluster_utils.migrator.MigratorGISSim;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCSync;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.Consts;
import graph_gen_utils.general.Utils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsBalanced;
import graph_gen_utils.partitioner.PartitionerAsDefault;
import graph_gen_utils.partitioner.PartitionerAsFile;
import graph_gen_utils.partitioner.PartitionerAsRandom;
import graph_gen_utils.partitioner.PartitionerAsSingle;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.core.PGraphDatabaseServiceImpl;
import p_graph_service.sim.PGraphDatabaseServiceSIM;

public class DodgyTests {

	public static void main(String[] args) {
		// String origFileName = "/home/something/a-b-c/name_asdfad.txt.txt";
		//
		// int slashEndIndex = (origFileName.lastIndexOf("/") == -1) ? 0
		// : origFileName.lastIndexOf("/");
		// String newSlashFileName = origFileName.substring(slashEndIndex + 1,
		// origFileName.length() - 1);
		// System.out.println(newSlashFileName);
		//
		// String slashFileDir = origFileName.substring(0, slashEndIndex);
		// System.out.println(slashFileDir);
		//
		// int dotEndIndex = (origFileName.indexOf(".") == -1) ? origFileName
		// .length() - 1 : origFileName.indexOf(".");
		// String newDotFileName = origFileName.substring(0, dotEndIndex);
		//
		// System.out.println(origFileName);
		// System.out.printf("%s_%d.txt\n", newDotFileName, System
		// .currentTimeMillis());

		test_migrator_sim();
	}

	private static void test_migrator_sim() {
		String testDir = "/home/alex/Desktop/Test/";
		String gmlAfterName = testDir + "small_gis_after.gml";
		String dbDir = testDir + "var/";
		String graphName = "/home/alex/workspace/graph_cluster_utils/graphs/test-cluster.gml";

		Utils.cleanDir(testDir);
		Utils.cleanDir(dbDir);

		GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
		// Partitioner partitioner = new PartitionerAsDefault();
		Partitioner partitioner = new PartitionerAsBalanced((byte) 2);
		NeoFromFile.writeNeoFromGMLAndPtn(db, graphName, partitioner);

		NeoFromFile.writeGMLFull(db, testDir + "test-cluster.gml");

		db.shutdown();

		PGraphDatabaseService pdb = new PGraphDatabaseServiceSIM(dbDir, 0);
		pdb.setDBChangeLog(String.format("%schange_op_log_%d.txt", testDir,
				System.currentTimeMillis()));

		MemGraph memDb = NeoFromFile.readMemGraph(pdb);

		int snapshotPeriod = 10;
		int longSnapshotPeriod = 10;
		Logger logger = new LoggerBase(snapshotPeriod, longSnapshotPeriod,
				"migrator_gis_test", testDir);

		LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

		int migrationPeriod = 25;
		Migrator migrator = new MigratorGISSim(pdb, migrationPeriod, changeLog,
				testDir);

		PtnAlg ptnAlg = new PtnAlgDiDiCSync(memDb, logger, changeLog, migrator);

		int maxIterations = 50;
		ConfDiDiC config = new ConfDiDiC((byte) 2);
		config.setMaxIterations(maxIterations);

		ptnAlg.doPartition(config);

		NeoFromFile.writeGMLBasic(pdb, gmlAfterName);

		pdb.shutdown();
	}

	private static void test_change_op_reader() {
		String testDir = "/home/alex/Desktop/Test/";
		String dbDir = testDir + "DB_Empty/";
		String pdbDir = testDir + "PDB_ChangeOpTest/";
		String graphName = "/home/alex/workspace/graph_cluster_utils/graphs/test0.graph";
		String changeOpLogName = dbDir + "changeOpLog.txt";

		Utils.cleanDir(testDir);
		Utils.cleanDir(dbDir);
		Utils.cleanDir(pdbDir);

		System.out.println(testDir);
		System.out.println(dbDir);
		System.out.println(pdbDir);

		GraphDatabaseService db = new EmbeddedGraphDatabase(dbDir);
		NeoFromFile.writeNeoFromChaco(db, graphName);

		// PGraphDatabaseService pdb = NeoFromFile.writePNeoFromNeo(pdbDir, db);
		db.shutdown();
		PGraphDatabaseService pdb = new PGraphDatabaseServiceSIM(dbDir, 0);

		System.out.println(pdb.getDBChangeLog());

		Transaction tx = pdb.beginTx();
		try {
			Node node1 = pdb.createNode();
			Node node2 = pdb.createNode();
			Relationship rel1 = node1.createRelationshipTo(node2,
					Consts.RelationshipTypes.DEFAULT);
			rel1.delete();
			node1.delete();
			tx.success();
		} catch (Exception e) {
			tx.failure();
			e.printStackTrace();
		} finally {
			tx.finish();
		}

		System.out.println(pdb.getDBChangeLog());

		File changeOpLogFile = new File(changeOpLogName);
		LogReaderChangeOp logReader = new LogReaderChangeOp(changeOpLogFile);

		System.out.println("Reading ChangeOp Log...");
		for (ChangeOp changeOp : logReader.getChangeOps()) {
			System.out.println(changeOp.toString());
		}

		// db.shutdown();
		pdb.shutdown();

	}

	private static void partition_tree() {
		String treeDir = "/home/alex/workspace/graph_cluster_utils/var/fs-tree/";
		String dbName = treeDir + "fs-tree-db";

		PGraphDatabaseServiceSIM treeDb = new PGraphDatabaseServiceSIM(dbName,
				0);

		MemGraph memTreeDb = NeoFromFile.readMemGraph(treeDb);

		int snapshotPeriod = 50;
		int longSnapshotPeriod = 50;
		Logger logger = new LoggerBase(snapshotPeriod, longSnapshotPeriod,
				"fs-tree", treeDir);

		LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

		int migrationPeriod = 50;
		Migrator migrator = new MigratorBase(treeDb, migrationPeriod);

		PtnAlg ptnAlg = new PtnAlgDiDiCSync(memTreeDb, logger, changeLog,
				migrator);

		int maxIterations = 101;
		ConfDiDiC config = new ConfDiDiC((byte) 2);
		config.setMaxIterations(maxIterations);

		ptnAlg.doPartition(config);

		treeDb.shutdown();
	}

	private static void test_migrator() {
		String testDir = "/home/alex/workspace/graph_cluster_utils/var/migrator test/";

		String dbName = testDir + "migrator_test_db";
		Utils.cleanDir(dbName);

		GraphDatabaseService db = new EmbeddedGraphDatabase(dbName);

		String chacoName = testDir + "input/migrator_test-IN.graph";
		String ptnName = testDir + "input/migrator_test-IN.2.ptn";
		NeoFromFile.writeNeoFromChacoAndPtn(db, chacoName, ptnName);
		db.shutdown();

		PGraphDatabaseServiceSIM userDb = new PGraphDatabaseServiceSIM(dbName,
				0);

		String userDbGmlNameBefore = "userDbGmlBefore.gml";
		NeoFromFile.writeGMLBasic(userDb, testDir + userDbGmlNameBefore);

		MemGraph memDb = NeoFromFile.readMemGraph(userDb);

		String memDbGmlNameBefore = "memDbGmlBefore.gml";
		NeoFromFile.writeGMLBasic(memDb, testDir + memDbGmlNameBefore);

		int snapshotPeriod = 50;
		int longSnapshotPeriod = 50;
		Logger logger = new LoggerBase(snapshotPeriod, longSnapshotPeriod,
				"migrator_test", testDir);

		LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

		int migrationPeriod = 50;
		Migrator migrator = new MigratorBase(userDb, migrationPeriod);

		PtnAlg ptnAlg = new PtnAlgDiDiCSync(memDb, logger, changeLog, migrator);

		int maxIterations = 51;
		ConfDiDiC config = new ConfDiDiC((byte) 2);
		config.setMaxIterations(maxIterations);

		ptnAlg.doPartition(config);

		String userDbGmlNameAfter = "userDbGmlAfter.gml";
		NeoFromFile.writeGMLBasic(userDb, testDir + userDbGmlNameAfter);

		String memDbGmlNameAfter = "memeDbGmlAfter.gml";
		NeoFromFile.writeGMLBasic(memDb, testDir + memDbGmlNameAfter);

		userDb.shutdown();
	}

	private static void run_algorithm() {
		// This method does the following:
		// ---> Loads a Neo4j instance
		// ---> Loads Neo4j into memory
		// ---> Creates and configures metrics Logger
		// ---> Creates change log
		// ---> Creates Migrator
		// ---> Creates and configures partitioning algorithm (PtnAlg)
		// ---> Runs PtnAlg

		try {
			// This is where Neo4j instance is located
			// TODO Implement E.g. "var/tree-graph/"
			String dbDirectory = "PUT YOUR DB DIRECTORY HERE";

			PGraphDatabaseServiceSIM db = new PGraphDatabaseServiceSIM(
					dbDirectory, 0);

			MemGraph memGraph = NeoFromFile.readMemGraph(db);

			// Name that will prepend metrics file names
			String graphName = "fs-tree";

			// Directory where metrics files will be written
			// TODO Implement E.g. "var/tree-graph-logs/"
			String resultsDirectory = "PUT YOUR RESULTS DIRECTORY HERE";

			// Deletes all files in Results directory
			Utils.cleanDir(resultsDirectory);

			Logger logger = new LoggerMetricsMinimal(graphName,
					resultsDirectory);

			// Change log, in this case it's always empty
			LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

			// Push changes to the original PGraphDatabaseService
			// TODO Define how often changes are pushed to PGraphDatabaseService
			int migrationPeriod = 100;
			Migrator migrator = new MigratorBase(db, migrationPeriod);

			PtnAlg ptnAlg = new PtnAlgDiDiCSync(memGraph, logger, changeLog,
					migrator);

			// This contains the algorithm parameters
			// TODO Number of partitions
			byte numberOfPartitions = 2;
			Conf config = new ConfDiDiC(numberOfPartitions);

			// Number of algorithm iterations
			// TODO Define max algorithm iterations
			int maxIterations = 101;
			((ConfDiDiC) config).setMaxIterations(maxIterations);

			ptnAlg.doPartition(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void db_service_comparison() {
		String graphName = "test-cluster";
		// String graphName = "add20";
		// String graphName = "m14b";

		int snapshotPeriod = 1;
		int iterations = 3;

		mem_neo(graphName, snapshotPeriod, iterations);
		normal_db(graphName, snapshotPeriod, iterations);
		partitioned_neo(graphName, snapshotPeriod, iterations);
	}

	private static void normal_db(String graphName, int snapshotPeriod,
			int iterations) {

		String graphDir = String.format("graphs/%s.graph", graphName);

		String normNeoDir = "var/normal-neo/";
		String normNeoResultsDir = "var/normal-neo-results/";

		Utils.cleanDir(normNeoDir);
		Utils.cleanDir(normNeoResultsDir);

		GraphDatabaseService normNeo = new EmbeddedGraphDatabase(normNeoDir);
		Partitioner partitioner = new PartitionerAsBalanced((byte) 2);

		NeoFromFile.writeNeoFromChacoAndPtn(normNeo, graphDir, partitioner);

		Logger logger = new LoggerBase(snapshotPeriod, snapshotPeriod,
				graphName, normNeoResultsDir);

		LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

		Migrator migrator = new MigratorDummy();

		// PtnAlg ptnAlg = new PtnAlgDiDiCBase(normNeo, logger, changeLog,
		// migrator);
		PtnAlg ptnAlg = new PtnAlgDiDiCSync(normNeo, logger, changeLog,
				migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCPaper(normNeo, logger, changeLog,
		// migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCBal(normNeo, logger, changeLog,
		// migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCFix(normNeo, logger, changeLog,
		// migrator);

		ConfDiDiC config = new ConfDiDiC((byte) 2);
		config.setMaxIterations(iterations);

		ptnAlg.doPartition(config);

		normNeo.shutdown();
	}

	private static void mem_neo(String graphName, int snapshotPeriod,
			int iterations) {

		// print_memory_status();
		// long usageBase = memUsage();

		String graphDir = String.format("graphs/%s.graph", graphName);

		String normNeoDir = "var/normal-neo/";
		String memNeoResultsDir = "var/mem-neo-results/";

		Utils.cleanDir(normNeoDir);
		Utils.cleanDir(memNeoResultsDir);

		GraphDatabaseService normNeo = new EmbeddedGraphDatabase(normNeoDir);
		Partitioner partitioner = new PartitionerAsBalanced((byte) 2);

		NeoFromFile.writeNeoFromChacoAndPtn(normNeo, graphDir, partitioner);

		// print_memory_status();
		// long usageNeo = memUsage();
		// System.out.println("Usage Neo: " + memUsageToStr(usageNeo -
		// usageBase));

		MemGraph memNeo = NeoFromFile.readMemGraph(normNeo);

		// print_memory_status();
		// long usageNeoAndMem = memUsage();
		// System.out.println("Usage Mem: "
		// + memUsageToStr(usageNeoAndMem - usageNeo));

		Logger logger = new LoggerBase(snapshotPeriod, snapshotPeriod,
				graphName, memNeoResultsDir);

		LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

		Migrator migrator = new MigratorDummy();

		// PtnAlg ptnAlg = new PtnAlgDiDiCBase(memNeo, logger, changeLog,
		// migrator);
		PtnAlg ptnAlg = new PtnAlgDiDiCSync(memNeo, logger, changeLog, migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCPaper(memNeo, logger, changeLog,
		// migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCBal(memNeo, logger, changeLog,
		// migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCFix(memNeo, logger, changeLog,
		// migrator);

		ConfDiDiC config = new ConfDiDiC((byte) 2);
		config.setMaxIterations(iterations);

		ptnAlg.doPartition(config);

		normNeo.shutdown();

	}

	private static void partitioned_neo(String graphName, int snapshotPeriod,
			int iterations) {
		String graphDir = String.format("graphs/%s.graph", graphName);

		String normNeoDir = "var/normal-neo/";
		String partNeoDir = "var/partitioned-neo/";
		String partNeoResultsDir = "var/partitioned-neo-results/";

		Utils.cleanDir(normNeoDir);
		Utils.cleanDir(partNeoDir);
		Utils.cleanDir(partNeoResultsDir);

		GraphDatabaseService normNeo = new EmbeddedGraphDatabase(normNeoDir);
		Partitioner partitioner = new PartitionerAsBalanced((byte) 2);

		NeoFromFile.writeNeoFromChacoAndPtn(normNeo, graphDir, partitioner);

		PGraphDatabaseService partNeo = NeoFromFile.writePNeoFromNeo(
				partNeoDir, normNeo);

		Logger logger = new LoggerBase(snapshotPeriod, snapshotPeriod,
				graphName, partNeoResultsDir);

		LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

		Migrator migrator = new MigratorDummy();

		// PtnAlg ptnAlg = new PtnAlgDiDiCBase(partNeo, logger, changeLog,
		// migrator);
		PtnAlg ptnAlg = new PtnAlgDiDiCSync(partNeo, logger, changeLog,
				migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCPaper(partNeo, logger, changeLog,
		// migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCBal(partNeo, logger, changeLog,
		// migrator);
		// PtnAlg ptnAlg = new PtnAlgDiDiCFix(partNeo, logger, changeLog,
		// migrator);

		ConfDiDiC config = new ConfDiDiC((byte) 2);
		config.setMaxIterations(iterations);

		ptnAlg.doPartition(config);

		normNeo.shutdown();
		partNeo.shutdown();
	}

	private static void run_in_thread() {
		try {
			Utils.cleanDir("var");
			Utils.cleanDir("var/algNeo");
			Utils.cleanDir("var/userNeo");
			Utils.cleanDir("var/algNeo-results");

			GraphDatabaseService transNeo = new EmbeddedGraphDatabase(
					"var/algNeo");

			Partitioner partitioner = new PartitionerAsFile(new File(
					"partitionings/test0-IN.2.ptn"));

			NeoFromFile.writeNeoFromChacoAndPtn(transNeo, "graphs/test0.graph",
					partitioner);

			MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

			Logger logger = new LoggerBase(1, 1, "test0", "var/algNeo-results/");

			LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();
			changeLog.put(new ChangeOpEnd());

			PGraphDatabaseService userTransNeo = new PGraphDatabaseServiceImpl(
					"var/userNeo", 1);

			Migrator migrator = new MigratorBase(userTransNeo,
					Integer.MAX_VALUE);

			PtnAlg ptnAlg = new PtnAlgDiDiCSync(memGraph, logger, changeLog,
					migrator);

			Conf config = new ConfDiDiC((byte) 2);

			AlgRunner algRunner = new AlgRunner(ptnAlg, config);
			algRunner.start();

			Thread.sleep(5000);
			changeLog.put(new ChangeOpDeleteRelationship(0));
			changeLog.put(new ChangeOpDeleteRelationship(4));
			changeLog.put(new ChangeOpDeleteNode(5));
			changeLog.put(new ChangeOpAddNode(6, (byte) 0));
			changeLog.put(new ChangeOpAddRelationship(60, 3, 6));
			changeLog.put(new ChangeOpAddNode(7, (byte) 0));
			changeLog.put(new ChangeOpAddRelationship(70, 6, 7));
			changeLog.put(new ChangeOpEnd());

			algRunner.join();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void print_memory_status() {

		System.out.printf("\n***Memory Usage***\n");

		String memoryStats = null;

		List<MemoryPoolMXBean> memoryPools = ManagementFactory
				.getMemoryPoolMXBeans();

		if (memoryPools != null) {

			Iterator<MemoryPoolMXBean> memoryPoolIterator = memoryPools
					.iterator();

			while (memoryPoolIterator.hasNext()) {

				MemoryPoolMXBean pool = memoryPoolIterator.next();

				if (pool != null) {

					MemoryUsage usage = pool.getUsage();
					if (usage != null) {

						memoryStats = (memoryStats != null ? memoryStats + "\n"
								: "")
								+ "\t"
								+ String.format("%1$-" + 17 + "s", pool
										.getName())
								+ " = "
								+ usage.getUsed()
								+ " (" + usage.getMax() + ")";
					}
				}
			}

		}

		System.out.println(memoryStats);

		System.out.printf("******************\n\n");
	}

	private static long memUsage() {
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		System.gc();
		return Runtime.getRuntime().totalMemory()
				- Runtime.getRuntime().freeMemory();
	}

	private static String memUsageToStr(long memUsageTotal) {
		long b = memUsageTotal % 1024;
		long k = (memUsageTotal / 1024) % 1024;
		long m = (memUsageTotal / 1024) / 1024;

		return String.format("%d(mb):%d(kb):%d(b)%n", m, k, b);
	}
}
