package graph_cluster_utils;

import graph_gen_utils.NeoFromFile;

public class SupervisorDiDiC extends Supervisor {

	private int snapshotPeriod = -1;
	private String graphName = "";
	private String graphDir = "";
	private String ptnDir = "";
	private String metDir = "";

	public SupervisorDiDiC(int snapshotPeriod, String graphName,
			String graphDir, String ptnDir, String metDir) {
		super();
		this.snapshotPeriod = snapshotPeriod;
		this.graphName = graphName;
		this.graphDir = graphDir;
		this.ptnDir = ptnDir;
		this.metDir = metDir;
	}

	@Override
	public boolean is_dynamism(int timeStep) {
		return false;
	}

	@Override
	public void do_dynamism(String databaseDir) {
	}

	@Override
	public boolean is_initial_snapshot() {
		return true;
	}

	@Override
	public void do_initial_snapshot(int clusterCount, String databaseDir) {

		try {
			String outMetrics = String.format("%s%s-INIT.%d.met", metDir,
					graphName, clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(databaseDir);

			// Write graph metrics to file
			neoCreator.generateMetrics(outMetrics);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean is_periodic_snapshot(int timeStep) {
		return (timeStep % snapshotPeriod) == 0;
	}

	@Override
	public void do_periodic_snapshot(int timeStep, int clusterCount,
			String databaseDir) {

		try {
			String outMetrics = String.format("%s%s-%d.%d.met", metDir,
					graphName, timeStep, clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(databaseDir);

			// Write graph metrics to file
			neoCreator.generateMetrics(outMetrics);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public boolean is_final_snapshot() {
		return true;
	}

	@Override
	public void do_final_snapshot(int clusterCount, String databaseDir) {

		try {
			String outGraph = String.format("%s%s-FINAL.graph", graphDir,
					graphName);
			String outPtn = String.format("%s%s-OUT-FINAL.%d.ptn", ptnDir,
					graphName, clusterCount);
			String outMetrics = String.format("%s%s-FINAL.%d.met", metDir,
					graphName, clusterCount);

			// Create NeoFromFile and assign DB location
			NeoFromFile neoCreator = new NeoFromFile(databaseDir);

			neoCreator.generateChaco(outGraph,
					NeoFromFile.ChacoType.UNWEIGHTED, outPtn);

			// Write graph metrics to file
			neoCreator.generateMetrics(outMetrics);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
