package applications;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.config.Conf;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.logger.LoggerBase;
import graph_cluster_utils.logger.LoggerMinimal;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.migrator.MigratorBase;
import graph_cluster_utils.migrator.MigratorSim;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCSync;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.Utils;
import graph_gen_utils.memory_graph.MemGraph;

import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import jobs.SimJob;
import jobs.SimJobLoadOpsGIS;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.sim.PGraphDatabaseServiceSIM;

public class ChurnPtnAlgRunner {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// This method does the following:
		// ---> Loads a Neo4j instance
		// ---> Loads Neo4j into memory
		// ---> Creates and configures metrics Logger
		// ---> Creates change log
		// ---> Creates Migrator
		// ---> Creates and configures partitioning algorithm (PtnAlg)
		// ---> Runs PtnAlg

		if (args[0].equals("help")) {
			System.out.println("Params - " + "GraphName:Str "
					+ "Partitions:Byte " + "AlgIters:Int " + "DBDirectory:Str "
					+ "ResultsDir:Str " + "DBSyncPeriod:Int");
			return;
		}

		PGraphDatabaseServiceSIM db = null;

		try {

			// Name that will prepend metrics file names
			String graphName = args[0]; // "fs-tree"

			// Algorithm parameter: partition count
			byte numberOfPartitions = Byte.parseByte(args[1]);

			// Algorithm parameter: Max iterations
			int maxIterations = Integer.parseInt(args[2]);

			// This is where Neo4j instance is located
			String dbDirectory = args[3]; // "var/tree-graph/"

			// Directory where metrics files will be written
			String resultsDirectory = args[4]; // "var/tree-graph-logs/"

			// How often changes are pushed to PGraphDatabaseService
			int migrationPeriod = Integer.parseInt(args[5]);

			// *************
			long time = System.currentTimeMillis();
			System.out.printf("Opening DB...");

			db = new PGraphDatabaseServiceSIM(dbDirectory, 0);

			System.out.printf("%s", getTimeStr(System.currentTimeMillis()
					- time));

			MemGraph memGraph = NeoFromFile.readMemGraph(db);

			Logger logger = new LoggerMinimal(graphName, resultsDirectory);

			// Change log, in this case it's always empty
			LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

			// Push changes to the original PGraphDatabaseService
			Migrator migrator = new MigratorBase(db, migrationPeriod);

			PtnAlg ptnAlg = new PtnAlgDiDiCSync(memGraph, logger, changeLog,
					migrator);

			Conf config = new ConfDiDiC(numberOfPartitions);

			// Number of algorithm iterations
			((ConfDiDiC) config).setMaxIterations(maxIterations);

			ptnAlg.doPartition(config);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (db != null)
				db.shutdown();
		}
	}

	private void temp(String[] args) {
		// This method does the following:
		// ---> Loads a Neo4j instance
		// ---> Loads Neo4j into memory
		// ---> Creates and configures metrics Logger
		// ---> Creates change log
		// ---> Creates migrator
		// ---> Creates simulation job and passes it to migrator
		// ---> Creates and configures partitioning algorithm (PtnAlg)
		// ---> Runs partitioning algorithm

		if (args[0].equals("help")) {
			System.out.println("Params - " + "GraphName:Str "
					+ "Partitions:Byte " + "AlgIters:Int " + "DBDirectory:Str "
					+ "ResultsDir:Str " + "DBSyncPeriod:Int");
			return;
		}
		if (args[0].equals("help")) {
			System.out.println("Params - " + "RelTypesToRemove:Tuple2 "
					+ "RandomRelDeleteParams:Array "
					+ "RandomNodeDeleteParams:Array "
					+ "RemoveDuplicateRelationships:Bool "
					+ "RemoveOrphanNodes:Bool " + "Neo4jDirectories:Array");
			System.out.println("E.g. - " + "{RelType1:RelType2:...:RelTypeN} "
					+ "{PercentRelToKeep:MaxId} " + "PercentNodeToKeep "
					+ "true " + "true " + "Neo4jDir1/ Neo4jDir2/ ...");
			return;
		}

		// *********************

		PGraphDatabaseServiceSIM db = null;

		try {

			// Name that will prepend metrics file names
			String graphName = args[0]; // "fs-tree"

			// Algorithm parameter: partition count
			byte numberOfPartitions = Byte.parseByte(args[1]);

			// Algorithm parameter: Max iterations
			int maxIterations = Integer.parseInt(args[2]);

			// This is where Neo4j instance is located
			String dbDirectory = args[3]; // "var/tree-graph/"

			// Directory where metrics files will be written
			String resultsDirectory = args[4]; // "var/tree-graph-logs/"

			// How often changes are pushed to PGraphDatabaseService
			int migrationPeriod = Integer.parseInt(args[5]);

			// Input Operation logs to be loaded by simulator
			String[] operationLogsIn = args[6].replaceAll("[{}]", "").split(
					"[:]");

			String operationLogsOutDir = args[7];

			// *****************************

			// graphName = "migrator_gis_test";
			// numberOfPartitions = (byte) 2;
			// maxIterations = 100;
			// dbDirectory = "var/";
			// resultsDirectory = "results/";
			// migrationPeriod = 25;
			// operationLogsIn = new String[] { "log1", "log2" };
			// operationLogsOutDir = resultsDirectory;

			// *****************************

			String changeLogPath = String.format("%schange_op_log_%d.txt",
					resultsDirectory, System.currentTimeMillis());
			int snapshotPeriod = 1;

			PGraphDatabaseService pdb = new PGraphDatabaseServiceSIM(
					dbDirectory, 0);

			pdb.setDBChangeLog(changeLogPath);

			MemGraph memDb = NeoFromFile.readMemGraph(pdb);

			Logger logger = new LoggerBase(snapshotPeriod, graphName,
					resultsDirectory);

			SimJob simJob = new SimJobLoadOpsGIS(operationLogsIn,
					operationLogsOutDir, pdb);

			LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

			Migrator migrator = new MigratorSim(pdb, migrationPeriod,
					changeLog, simJob);

			PtnAlg ptnAlg = new PtnAlgDiDiCSync(memDb, logger, changeLog,
					migrator);

			ConfDiDiC config = new ConfDiDiC(numberOfPartitions);
			config.setMaxIterations(maxIterations);

			ptnAlg.doPartition(config);

			pdb.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (db != null)
				db.shutdown();
		}

	}

	private static String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}
}
