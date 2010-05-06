package dodgy_tests;

import graph_cluster_utils.alg_runner.AlgRunner;
import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.change_log.ChangeOpAddNode;
import graph_cluster_utils.change_log.ChangeOpAddRelationship;
import graph_cluster_utils.change_log.ChangeOpDeleteNode;
import graph_cluster_utils.change_log.ChangeOpDeleteRelationship;
import graph_cluster_utils.change_log.ChangeOpEnd;
import graph_cluster_utils.config.Conf;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.logger.LoggerBase;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.migrator.MigratorBase;
import graph_cluster_utils.migrator.MigratorDummy;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCBal;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCBase;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCFix;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCPaper;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCSync;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.DirUtils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsBalanced;
import graph_gen_utils.partitioner.PartitionerAsFile;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.core.PGraphDatabaseServiceImpl;

public class DodgyTests {

	public static void main(String[] args) {
		db_service_comparison();
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

		DirUtils.cleanDir(normNeoDir);
		DirUtils.cleanDir(normNeoResultsDir);

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

		DirUtils.cleanDir(normNeoDir);
		DirUtils.cleanDir(memNeoResultsDir);

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

		DirUtils.cleanDir(normNeoDir);
		DirUtils.cleanDir(partNeoDir);
		DirUtils.cleanDir(partNeoResultsDir);

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
			DirUtils.cleanDir("var");
			DirUtils.cleanDir("var/algNeo");
			DirUtils.cleanDir("var/userNeo");
			DirUtils.cleanDir("var/algNeo-results");

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
