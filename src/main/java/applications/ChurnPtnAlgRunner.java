package applications;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.logger.LoggerBase;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.migrator.MigratorSim;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCSync;
import graph_cluster_utils.ptn_alg.didic.config.ConfDiDiC;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.memory_graph.MemGraph;

import java.util.concurrent.LinkedBlockingQueue;

import jobs.SimJob;
import jobs.SimJobLoadOpsGIS;
import jobs.SimJobLoadOpsTree;
import jobs.SimJobLoadOpsTwitter;

import p_graph_service.PGraphDatabaseService;
import p_graph_service.sim.PGraphDatabaseServiceSIM;

public class ChurnPtnAlgRunner {

	private enum SimType {
		TWITTER, GIS, FSTREE
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
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
			System.out.println("Params - " +

			"GraphName:Str " + "Partitions:Byte " + "AlgIters:Int "
					+ "DBDirectory:Str " + "ResultsDir:Str "
					+ "DBSyncPeriod:Int " + "MetricsLogPeriod:Int "
					+ "InputReadOpsFile:String " + "OutputReadOpsDir:String "
					+ "ChangeOpLogFiles:Array "
					+ "SimType:Enum(gis,fstree,twitter) ");

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

			// How often metrics are calculated & logged
			int metricsPeriod = Integer.parseInt(args[6]);

			// Input Operation logs to be loaded by simulator
			String readOperationLogIn = args[7];

			// Directory simulator should write result logs
			String operationLogsOutDir = args[8];

			String[] changeOpLogFilesArg = args[9].replaceAll("[{}]", "")
					.split("[:]");
			String[] changeOpLogFiles = new String[changeOpLogFilesArg.length * 2];
			for (int i = 0; i < changeOpLogFilesArg.length; i++) {
				// Make sure every 2nd ChangeOpLog is a DUMMY empty log
				// This allows for calling Migrator twice per DiDiC iteration
				changeOpLogFiles[i * 2] = changeOpLogFilesArg[i];
				changeOpLogFiles[i * 2 + 1] = null;
			}

			String simTypeStr = args[10];
			SimType simType = null;
			if (simTypeStr.equals("twitter") == true) {
				simType = SimType.TWITTER;
			} else if (simTypeStr.equals("gis") == true) {
				simType = SimType.GIS;
			} else if (simTypeStr.equals("fstree") == true) {
				simType = SimType.FSTREE;
			} else {
				String errStr = String.format(
						"Invalid SimType[%s], must be (twitter,gis,fstree)\n",
						simTypeStr);
				throw new Exception(errStr);
			}

			// *****************************

			// graphName = "migrator_gis_test";
			// numberOfPartitions = (byte) 2;
			// maxIterations = 100;
			// dbDirectory = "var/";
			// resultsDirectory = "results/";
			// migrationPeriod = 25;
			// metricsPeriod = 1;
			// operationLogsIn = "inputLogsDirectory/inputLog.txt";
			// operationLogsOutDir = "resultLogsDirectory/";
			// changeopLogFile = "{file1.txt:file2.txt:file3.txt}"

			// *****************************

			PGraphDatabaseService pdb = new PGraphDatabaseServiceSIM(
					dbDirectory, 0);

			MemGraph memDb = NeoFromFile.readMemGraph(pdb);

			Logger logger = new LoggerBase(metricsPeriod, graphName,
					resultsDirectory);

			SimJob simJob = null;

			switch (simType) {
			case TWITTER:
				simJob = new SimJobLoadOpsTwitter(
						new String[] { readOperationLogIn },
						operationLogsOutDir, pdb, true);
				break;
			case GIS:
				simJob = new SimJobLoadOpsGIS(
						new String[] { readOperationLogIn },
						operationLogsOutDir, pdb, true);
				break;
			case FSTREE:
				simJob = new SimJobLoadOpsTree(
						new String[] { readOperationLogIn },
						operationLogsOutDir, pdb, true);
				break;
			}

			LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();

			Migrator migrator = new MigratorSim(pdb, migrationPeriod,
					changeLog, simJob, changeOpLogFiles);

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

}
