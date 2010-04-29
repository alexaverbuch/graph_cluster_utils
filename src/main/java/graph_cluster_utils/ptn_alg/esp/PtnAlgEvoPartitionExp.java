package graph_cluster_utils.ptn_alg.esp;

import graph_cluster_utils.change_log.ChangeOp;
import graph_cluster_utils.config.Conf;
import graph_cluster_utils.logger.Logger;
import graph_cluster_utils.migrator.Migrator;
import graph_cluster_utils.ptn_alg.esp.config.ConfEvoPartition;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * WARNING: Not in a usable state!
 * 
 * Inherits from {@link PtnAlgEvoPartition}.
 * 
 * This is a simplified implementation of the Evolving Set Process
 * clustering/partitioning algorithm.
 * 
 * The number of parameters has been reduced to only what is deemed necessary.
 * Some variants have been relaxed and/or removed. Also, a modification has been
 * added to allocate unallocated nodes to partitions/clusters after the
 * algorithm terminates.
 * 
 * Changes made to this implementation are on based on suggestions made by the
 * algorithm author(s) during ongoing email communications.
 * 
 * This implementation is still not mature enough to be used. It's success is
 * too topology dependent and more debugging/experimenting is necessary.
 * 
 * It is computed directly on a Neo4j instance.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class PtnAlgEvoPartitionExp extends PtnAlgEvoPartition {

	public PtnAlgEvoPartitionExp(GraphDatabaseService transNeo, Logger logger,
			LinkedBlockingQueue<ChangeOp> changeLog, Migrator migrator) {
		super(transNeo, logger, changeLog, migrator);
	}

	@Override
	public void doPartition(Conf baseConfig) {
		this.config = (ConfEvoPartition) baseConfig;

		this.logger.doInitialSnapshot(transNeo, config);

		this.nodes = getNodeDegrees();

		evoPartition(config.getConductance(), config.getP());

		this.logger.doFinalSnapshot(transNeo, config);
	}

	// p is used to find jMax. Smaller p -> larger jMax
	private void evoPartition(Double conductance, Double p) {

		// Set W(j) = W(0) = V
		Long volumeG = getVolumeG((byte) -1);

		Long m = volumeG / 2;

		// Set j = 0
		Integer j = 0;

		// [WHILE] j < 12.m.Ceil( lg(1/p) ) [AND] volumeWj >= (3/4)volumeG
		Double jMax = 12 * m * Math.ceil(Math.log10(1.0 / p));

		Long volumeWj = volumeG;

		// NOTE Experimental!
		Long minClusterVolume = volumeG / config.getClusterCount();

		System.out
				.printf(
						"evoPartition[conduct=%f,p=%f,clusterCount=%d,minClusterVolume=%d]\n",
						conductance, p, config.getClusterCount(),
						minClusterVolume);

		// while ((j < jMax) && (volumeWj >= (3.0 / 4.0) * (double) volumeG)) {
		while (volumeWj >= (1.0 / 4.0) * (double) volumeG) {

			// System.out.printf(
			// "evoPartition[j=%d,jMax=%f,volWj=%d, 3/4volG[%f]]\n%s\n",
			// j, jMax, volumeWj, (3.0 / 4.0) * (double) volumeG,
			// nodesToStr());
			System.out.printf(
					"evoPartition[j=%d,jMax=%f,volWj=%d, 1/4volG[%f]]\n%s\n",
					j, jMax, volumeWj, (1.0 / 4.0) * (double) volumeG,
					nodesToStr());

			Transaction tx = transNeo.beginTx();

			try {

				// -> D(j) = evoNibble(G[W(j-1)], conductance)
				DataStructEvoPartition Dj = evoNibble(conductance, volumeWj,
						minClusterVolume);

				// -> Set j = j+1
				j++;

				if (Dj != null) {
					System.out.printf(
							"\n\tevoNibble returned. |D%d|[%d], volD%d[%d]\n",
							j, Dj.getS().size(), j, Dj.getVolume());
					System.out.printf("\t%s\n\n", dToStr(Dj.getS()));

					// -> W(j) = W(j-1) - D(j)
					clusterColor++;
					updateClusterAlloc(Dj.getS(), clusterColor);

					volumeWj -= Dj.getVolume();

					tx.success();
				} else
					System.out.printf(
							"\n\tevoNibble returned. D(%d) = null!\n\n", j);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				tx.finish();
			}

		}

		// Tidy up the unallocated vertices here
		allocateUnallocated();

		System.out
				.printf(
						"\nevoPartition[volWj=%d, 1/4volG[%f], volG[%d]]\n%s\n\n",
						volumeWj, (1.0 / 4.0) * (double) volumeG, volumeG,
						nodesToStr());

		// Set D = D(1) U ... U D(j)
		// In this implementation vertices are colored rather than removed,
		// so there is no need to perform a Union operation
	}

	private DataStructEvoPartition evoNibble(Double conductance, Long volumeWj,
			Long minClusterVolume) {

		// Precompute for efficiency
		Double log2VolumeWj = Math.log(volumeWj) / Math.log(2);
		Double logVolumeWj = Math.log(volumeWj);

		// T = Floor(conductance^-1 / 100)
		// Double T = Math.floor(Math.pow(conductance, -1) / 100.0);
		Double T = (double) 500;

		// thetaT = Sqrt(4.T^-1.log(volume(G)) )
		Double thetaT = Math.sqrt(4.0 * Math.pow(T, -1) * logVolumeWj);

		// Choose random vertex with probability P(X=x) = d(x)/volume(G)
		Node v = getRandomNode();

		// This can only happen if all nodes have been partitioned
		if (v == null)
			return null;

		// Choose random budget
		// -> Let jMax = Ceil(log_2(volumeG))
		Double jMax = Math.ceil(log2VolumeWj);

		// -> Let j be an integer in the range [0,jMax]
		// -> Choose budget with probability P(J=j) = constB.2^-j
		// ----> Where constB is a proportionality constant
		// NOTE Exponential distribution has similar charateristics
		Double j = expGenB.nextValue() * jMax;
		if (j > jMax) // Exponential distribution may return > 1.0
			j = 0.0; // If so, default to most probable j value

		// -> Let Bj = 8.y.2^j
		// ----> Where y = 1 + 4.Sqrt(T.log_2(volumeG))
		Double y = 1 + (4 * Math.sqrt(T * logVolumeWj));
		Double Bj = 8 * y * Math.pow(2, j);

		System.out.printf("\t\tevoNibble[conduct=%f,volG=%d]\n", conductance,
				volumeWj);
		System.out.printf("\t\t         [v=%d,j=%f,jMax=%f,y=%f,Bj=%f]\n", v
				.getId(), j, jMax, y, Bj);

		DataStructEvoPartition sAndB = genSample(v, T, Bj, thetaT,
				minClusterVolume);

		System.out
				.printf(
						"\t\t<evoNibble> genSample() -> S: conductS[%f]<=3thetaT[%f] , minVol[%d]<volS[%d]<=3/4volG[%f]\n",
						sAndB.getConductance(), 3 * thetaT, minClusterVolume,
						sAndB.getVolume(), (3.0 / 4.0) * volumeWj);

		// NOTE Experimental!
		// if ((sAndB.getConductance() <= 3 * thetaT)
		// && (sAndB.getVolume() > minClusterVolume)
		// && (sAndB.getVolume() <= (3.0 / 4.0) * volumeWj))
		// return sAndB;
		if ((sAndB.getVolume() > minClusterVolume)
				&& (sAndB.getVolume() <= (3.0 / 4.0) * volumeWj))
			return sAndB;

		return null;
	}

	// Simulates volume-biased Evolving Set Process
	// Updates boundary of current set at each step
	// Generates sample path of sets and outputs last set
	//
	// INPUT
	// ----> x : Starting vertex
	// ----> T>=0 : Time limit
	// ----> B>=0 : Budget
	// OUTPUT
	// ----> St : Set sampled from volume-biased Evolving Set Process
	private DataStructEvoPartition genSample(Node x, Double T, Double B,
			Double thetaT, Long minClusterVolume) {

		System.out
				.printf(
						"\t\t\tgenSample[x=%d,T=%f,B=%f,thetaT=%f,minClusterVolume=%d]\n",
						x.getId(), T, B, thetaT, minClusterVolume);

		DataStructEvoPartition sAndB = null; // S, B, volume, conductance
		Node X = null; // Current random-walk position @ time t
		Double Z = new Double(0);
		HashMap<Node, Boolean> D = null;

		// Inititialization
		// -> X = x0 = x
		X = x;
		// -> S = S0 = {x}
		sAndB = new DataStructEvoPartition(X, rng);

		sAndB.printSAndB();

		System.out
				.printf(
						"\t\t\t<genSample> SB_Init: conductS=%f, costS=%d, volS=%d, X=%d\n",
						sAndB.getConductance(), sAndB.getCost(), sAndB
								.getVolume(), X.getId());

		try {
			// ForEach Step t <= T
			for (int t = 0; t < T; t++) {

				// STAGE 1: compute St-1 to St difference

				// -> X = Choose X with p(Xt-1,Xt)
				X = sAndB.getNextV(X);

				// -> Compute probYinS(X)
				// -> Select random threshold Z = uniformRand[0,probNodeInS(v)]
				Z = sAndB.getZ(X);

				System.out.printf(
						"\t\t\t<genSample> t[%d], nextX[%d], Z[%f]\n", t, X
								.getId(), Z);

				// -> Define St = {y | probYinS(y,St-1) >= Z}
				// -> D = Set different between St & St-1
				// -> Update volume(St) & cost(S0,...,St)
				// -> Add/remove vertices in D to/from St
				// -> Update B(St-1) to B(St)
				D = sAndB.updateBoundary(Z, transNeo);

				// -> IF t==T OR cost()>B RETURN St = St-1 Diff D
				System.out.printf(
						"\t\t\t<genSample> Phase1? costS[%d] > B[%f]\n", sAndB
								.getCost(), B);

				// NOTE Experimental!
				// if (sAndB.getCost() > B) {
				//
				// sAndB.printSAndB();
				//
				// break;
				// }

				// STAGE 2: update S to St by adding/removing vertices in D to S

				// -> Compute conductance(St) = outDeg(St) / vol(St)
				// -> IF conductance(St) < thetaT RETURN St
				System.out
						.printf(
								"\t\t\t<genSample> Phase2? conductS[%f] < thetaT[%f]\n",
								sAndB.getConductance(), thetaT);

				sAndB.printSAndB();

				// NOTE Experimental!
				// if ((sAndB.getConductance() < thetaT)
				// && (sAndB.getVolume() > clusterVolume))
				if (sAndB.getVolume() > minClusterVolume)
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sAndB;
	}

}
