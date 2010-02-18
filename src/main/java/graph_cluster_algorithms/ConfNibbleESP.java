package graph_cluster_algorithms;

public class ConfNibbleESP {
	
	private Double theta = new Double(0); // TODO better default value
	private Double p = new Double(0); // TODO better default value
		
	public ConfNibbleESP() {
	}
	
	public ConfNibbleESP(Double theta, Double p) {
		super();
		this.theta = theta;
		this.p = p;
	}

	public Double getTheta() {
		return theta;
	}

	public void setTheta(Double theta) {
		this.theta = theta;
	}

	public Double getP() {
		return p;
	}

	public void setP(Double p) {
		this.p = p;
	}

}
