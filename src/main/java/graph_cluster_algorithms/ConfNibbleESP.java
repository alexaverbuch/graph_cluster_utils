package graph_cluster_algorithms;

public class ConfNibbleESP {
	
	private Double theta = new Double(0); // TODO better default value
	private Double p = new Double(0); // TODO better default value
		
	public ConfNibbleESP() {
	}
	
	public ConfNibbleESP(Double theta, Double p) throws Exception {
		super();
		
		if (theta > 1.0)
			throw new Exception("theta must be in range [0,1]!");
		if (p > 1.0)
			throw new Exception("p must be in range [0,1]!");
		
		this.theta = theta;
		this.p = p;
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
