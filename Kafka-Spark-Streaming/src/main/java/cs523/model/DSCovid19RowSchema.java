package cs523.model;

import java.io.Serializable;

import cs523.model.Covid19Row.Covid19RowBuilder;

public class DSCovid19RowSchema implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String rowKey;
	private String dtTracked;
	private String state;
	private String county;
	private int fipCode;
	private long nbrCases;
	private long nbrDeaths;
	
	public DSCovid19RowSchema() {}

	public DSCovid19RowSchema(Covid19RowBuilder builder) {
		this.rowKey = builder.rowKey();
		this.dtTracked = builder.dtTracked();
		this.state = builder.state();
		this.county = builder.county();
		this.fipCode = builder.fipCode();
		this.nbrCases = builder.nbrCases();
		this.nbrDeaths = builder.nbrDeaths();
	}

	public String getRowKey() {
		return rowKey;
	}

	public String getDtTracked() {
		return dtTracked;
	}

	public String getState() {
		return state;
	}

	public String getCounty() {
		return county;
	}

	public int getFipCode() {
		return fipCode;
	}

	public long getNbrCases() {
		return nbrCases;
	}

	public long getNbrDeaths() {
		return nbrDeaths;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public void setDtTracked(String dtTracked) {
		this.dtTracked = dtTracked;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void setCounty(String county) {
		this.county = county;
	}

	public void setFipCode(int fipCode) {
		this.fipCode = fipCode;
	}

	public void setNbrCases(long nbrCases) {
		this.nbrCases = nbrCases;
	}

	public void setNbrDeaths(long nbrDeaths) {
		this.nbrDeaths = nbrDeaths;
	}
	
}
