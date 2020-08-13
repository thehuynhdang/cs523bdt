package kafka.producer.api;

public class CovidCaseRecordLine {
	private String dtTracked;
	private String state;
	private String county;
	private int fipCode;
	private int nbrCases;
	private int nbrDeaths;

	public String getDtTracked() {
		return dtTracked;
	}
	public void setDtTracked(String dtTracked) {
		this.dtTracked = dtTracked;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCounty() {
		return county;
	}
	public void setCounty(String county) {
		this.county = county;
	}
	public int getFipCode() {
		return fipCode;
	}
	public void setFipCode(int fipCode) {
		this.fipCode = fipCode;
	}
	public int getNbrCases() {
		return nbrCases;
	}
	public void setNbrCases(int nbrCases) {
		this.nbrCases = nbrCases;
	}
	public int getNbrDeaths() {
		return nbrDeaths;
	}
	public void setNbrDeaths(int nbrDeaths) {
		this.nbrDeaths = nbrDeaths;
	}
	@Override
	public String toString() {
		return "CovidCaseRecordLine [state=" + state + ", county=" + county + ", fipsCode=" + fipCode + ", nbrCases="
				+ nbrCases + ", nbrDeaths=" + nbrDeaths + "]";
	}
	
}
