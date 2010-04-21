package dodgy_tests;

import graph_cluster_utils.alg_runner.AlgRunner;
import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.change_log.ChangeOpAddNode;
import graph_cluster_utils.change_log.ChangeOpAddRelationship;
import graph_cluster_utils.change_log.ChangeOpDeleteNode;
import graph_cluster_utils.change_log.ChangeOpDeleteRelationship;
import graph_cluster_utils.change_log.ChangeOpEnd;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.logger.LoggerBase;
import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.config.Conf;
import graph_cluster_utils.ptn_alg.config.ConfDiDiC;
import graph_cluster_utils.ptn_alg.didic.PtnAlgDiDiCSync;
import graph_gen_utils.NeoFromFile;
import graph_gen_utils.general.DirUtils;
import graph_gen_utils.memory_graph.MemGraph;
import graph_gen_utils.partitioner.Partitioner;
import graph_gen_utils.partitioner.PartitionerAsFile;
import graph_gen_utils.partitioner.PartitionerAsRandom;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class DodgyTests {

	public static void main(String[] args) {
		run_in_thread();
	}

	private static void run_in_thread() {
		try {
			DirUtils.cleanDir("var");
			DirUtils.cleanDir("var/test0");
			DirUtils.cleanDir("var/test0-results");

			GraphDatabaseService transNeo = new EmbeddedGraphDatabase(
					"var/test0");

			Partitioner partitioner = new PartitionerAsFile(new File(
					"partitionings/test0-IN.2.ptn"));

			NeoFromFile.writeNeoFromChacoAndPtn(transNeo, "graphs/test0.graph",
					partitioner);

			MemGraph memGraph = NeoFromFile.readMemGraph(transNeo);

			Logger logger = new LoggerBase(1, 1, "test0", "var/test0-results/");

			LinkedBlockingQueue<ChangeOp> changeLog = new LinkedBlockingQueue<ChangeOp>();
			changeLog.put(new ChangeOpEnd());

			PtnAlg ptnAlg = new PtnAlgDiDiCSync(memGraph, logger, changeLog);

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
}
