package cs523.model;

import java.io.Serializable;

import cs523.model.Covid19Row.Covid19RowBuilder;

public class SumCasesByDateRow implements Serializable{
	private static final long serialVersionUID = 1L;

	private String dtTracked;
	private String state;
	private long nbrCases;
	private long nbrDeaths;
	
	public SumCasesByDateRow() {}

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

	public long getNbrCases() {
		return nbrCases;
	}

	public void setNbrCases(long nbrCases) {
		this.nbrCases = nbrCases;
	}

	public long getNbrDeaths() {
		return nbrDeaths;
	}

	public void setNbrDeaths(long nbrDeaths) {
		this.nbrDeaths = nbrDeaths;
	}

	public Covid19Row toCovid19Row() {
		System.out.println(toString());
		return new Covid19RowBuilder()
				.dtTracked(this.dtTracked)
				.state(this.state)
				.nbrCases(this.nbrCases)
				.nbrDeaths(this.nbrDeaths)
				.build();
	}

	@Override
	public String toString() {
		return "SumCasesByDateRow [dtTracked=" + dtTracked + ", state=" + state
				+ ", nbrCases=" + nbrCases + ", nbrDeaths=" + nbrDeaths + "]";
	}
}
