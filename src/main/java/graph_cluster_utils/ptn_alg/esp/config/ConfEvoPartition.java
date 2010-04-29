package graph_cluster_utils.ptn_alg.esp.config;

import graph_cluster_utils.config.Conf;

/**
 * Inherits from {@link Conf}. Contains configuration parameters for the
 * Evolving Set Process clustering/partitioning algorithm.
 * 
 * @author Alex Averbuch
 * @since 2010-04-01
 */
public class ConfEvoPartition extends Conf {

	private Double theta = new Double(0); // TODO better default value
	private Double p = new Double(0); // TODO better default value
	private Double conductance = new Double(0); // TODO better default value
	private Long clusterCount = new Long(1);

	public ConfEvoPartition() {
	}

	public ConfEvoPartition(Double theta, Double p) throws Exception {
		super();

		if ((theta < 0.0) || (theta > 1.0))
			throw new Exception("theta must be in range [0,1]!");
		if ((p < 1.0) || (p > 1.0))
			throw new Exception("p must be in range [0,1]!");

		this.theta = theta;
		this.p = p;
	}

	public ConfEvoPartition(Double conductance) throws Exception {
		super();

		if ((conductance < 0.0) || (conductance > 1.0))
			throw new Exception("conductance must be in range [0,1]!");

		this.conductance = conductance;
	}

	public Double getConductance() {
		return conductance;
	}

	public Long getClusterCount() {
		return clusterCount;
	}

	public void setClusterCount(Long minClusterVolume) {
		this.clusterCount = minClusterVolume;
	}

	public void setConductance(Double conductance) {
		this.conductance = conductance;
	}

	public Double getTheta() {
		return theta;
	}

	public void setTheta(Double theta) throws Exception {
		if (theta > 1.0)
			throw new Exception("theta must be in range [0,1]!");
		this.theta = theta;
	}

	public Double getP() {
		return p;
	}

	public void setP(Double p) throws Exception {
		if (p > 1.0)
			throw new Exception("p must be in range [0,1]!");
		this.p = p;
	}

	@Override
	public String toString() {
		return String.format("K%d p%f cond", clusterCount, p, conductance);
	}

	@Override
	public String toStringDetailed() {
		return toString();
	}

}
