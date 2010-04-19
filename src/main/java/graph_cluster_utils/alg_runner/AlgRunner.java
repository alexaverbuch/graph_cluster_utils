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

	private void interruptExamples() {
		// InterruptedException

		// thread.interrupt() sets "interrupt status" flag

		// Thread.interrupted() checks "interrupt status" flag and clears it

		// thread.isInterrupted. query "interrupt status" of other thread
		// doesn't change the "interrupt status" flag

		// NOTE ***CATCH*** InterruptedException
		// for (int i = 0; i < importantInfo.length; i++) {
		// //Pause for 4 seconds
		// try {
		// Thread.sleep(4000);
		// } catch (InterruptedException e) {
		// //We've been interrupted: no more messages.
		// return;
		// }
		// //Print a message
		// System.out.println(importantInfo[i]);
		// }

		// NOTE ***POLL*** for interrupt now throws InterruptException
		// for (int i = 0; i < inputs.length; i++) {
		// heavyCrunch(inputs[i]);
		// if (Thread.interrupted()) {
		// //We've been interrupted: no more crunching.
		// return;
		// }
		// }
		// NOTE OR
		// if (Thread.interrupted()) {
		// throw new InterruptedException();
		// }
	}
}
