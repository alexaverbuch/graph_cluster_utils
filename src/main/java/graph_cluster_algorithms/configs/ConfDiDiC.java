package graph_cluster_algorithms.configs;

public class ConfDiDiC {

	// DiDiC Related Constants
	private int FOSTIterations = 11; // Primary Diffusion
	private int FOSBIterations = 11; // Secondary Diffusion ('drain')
	private int benefitLow = 1; // Benefit, used by FOS/B
	private int benefitHigh = 10; // Benefit, used by FOS/B
	private int defClusterVal = 100; // Default Init Val

	// DiDiC Related General Variables
	private int maxIterations = 150;
	private byte clusterCount = 2;

	// Experimental DiDiC Related
	public enum AllocType {
		BASE, OPT, HYBRID
	}

	// Experimental DiDiC Related
	private AllocType allocType = AllocType.BASE;
	private int hybridSwitchPoint = -1;
	private long clusterSizeOff = 0;
	private long clusterSizeOn = 0;

	public ConfDiDiC(byte clusterCount) {
		this.FOSTIterations = 11;
		this.FOSBIterations = 11;
		this.benefitLow = 1;
		this.benefitHigh = 10;
		this.defClusterVal = 100;

		this.maxIterations = 150;
		this.clusterCount = clusterCount;

		this.allocType = AllocType.BASE;
		this.hybridSwitchPoint = -1;
		this.clusterSizeOff = 0;
		this.clusterSizeOn = 0;
	}

	public ConfDiDiC(int fOSTIterations, int fOSBIterations, int benefitLow,
			int benefitHigh, int defClusterVal, int maxIterations,
			byte clusterCount, AllocType allocType, int hybridSwitchPoint,
			long clusterSizeOff, long clusterSizeOn) throws Exception {
		super();
		FOSTIterations = fOSTIterations;
		FOSBIterations = fOSBIterations;
		this.benefitLow = benefitLow;
		this.benefitHigh = benefitHigh;
		this.defClusterVal = defClusterVal;
		this.maxIterations = maxIterations;
		this.clusterCount = clusterCount;
		this.allocType = allocType;
		this.hybridSwitchPoint = hybridSwitchPoint;
		this.clusterSizeOff = clusterSizeOff;
		this.clusterSizeOn = clusterSizeOn;
	}

	public long getClusterSizeOff() {
		return clusterSizeOff;
	}

	public void setClusterSizeOff(long clusterSizeOff) {
		this.clusterSizeOff = clusterSizeOff;
	}

	public long getClusterSizeOn() {
		return clusterSizeOn;
	}

	public void setClusterSizeOn(long clusterSizeOn) {
		this.clusterSizeOn = clusterSizeOn;
	}

	public int getFOSTIterations() {
		return FOSTIterations;
	}

	public void setFOSTIterations(int fOSTIterations) {
		FOSTIterations = fOSTIterations;
	}

	public int getFOSBIterations() {
		return FOSBIterations;
	}

	public void setFOSBIterations(int fOSBIterations) {
		FOSBIterations = fOSBIterations;
	}

	public int getBenefitLow() {
		return benefitLow;
	}

	public void setBenefitLow(int benefitLow) {
		this.benefitLow = benefitLow;
	}

	public int getBenefitHigh() {
		return benefitHigh;
	}

	public void setBenefitHigh(int benefitHigh) {
		this.benefitHigh = benefitHigh;
	}

	public int getDefClusterVal() {
		return defClusterVal;
	}

	public void setDefClusterVal(int defClusterVal) {
		this.defClusterVal = defClusterVal;
	}

	public int getMaxIterations() {
		return maxIterations;
	}

	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public int getClusterCount() {
		return clusterCount;
	}

	public void setClusterCount(byte clusterCount) {
		this.clusterCount = clusterCount;
	}

	public AllocType getAllocType() {
		return allocType;
	}

	public void setAllocType(AllocType allocType) {
		this.allocType = allocType;
	}

	public int getHybridSwitchPoint() {
		return hybridSwitchPoint;
	}

	public void setHybridSwitchPoint(int hybridSwitchPoint) {
		this.hybridSwitchPoint = hybridSwitchPoint;
	}

}
