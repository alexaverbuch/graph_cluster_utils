package graph_cluster_utils.alg_runner;

import graph_cluster_utils.ptn_alg.PtnAlg;
import graph_cluster_utils.ptn_alg.config.Conf;

public class AlgRunner extends Thread {

	private PtnAlg ptnAlg = null;
	private Conf ptnAlgConfig = null;

	public AlgRunner(PtnAlg ptnAlg, Conf config) {
		this.ptnAlg = ptnAlg;
		this.ptnAlgConfig = config;
	}

	@Override
	public void run() {
		ptnAlg.doPartition(ptnAlgConfig);
		super.run();
	}
}
