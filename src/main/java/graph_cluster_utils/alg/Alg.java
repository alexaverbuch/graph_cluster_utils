package graph_cluster_utils.alg;

import graph_cluster_utils.alg.config.Conf;

/**
 * Interface all clustering/partitioning algorithms must implement
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public interface Alg {

	/**
	 * Run the clustering/partitioning algorithm
	 * 
	 * @param config
	 *            an implementation of {@link Conf} containing algorithm
	 *            specific configuration parameters
	 */
	public void start(Conf config);
}
