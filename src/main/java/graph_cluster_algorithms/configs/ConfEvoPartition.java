package graph_cluster_algorithms.configs;

public class ConfEvoPartition {

	private Double theta = new Double(0); // TODO better default value
	private Double p = new Double(0); // TODO better default value
	private Double conductance = new Double(0); // TODO better default value

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

}