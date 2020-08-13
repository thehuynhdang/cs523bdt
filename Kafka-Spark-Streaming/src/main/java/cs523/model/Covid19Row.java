package cs523.model;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

public class Covid19Row extends HRowKey implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private byte[] dtTracked;
	private byte[] state;
	private byte[] county;
	private byte[] fipCode;
	private byte[] nbrCases;
	private byte[] nbrDeaths;
	
	private Covid19Row(Covid19RowBuilder builder) {
		super(Bytes.toBytes(builder.rowKey()));
		this.dtTracked = Bytes.toBytes(builder.dtTracked());
		this.state = Bytes.toBytes(builder.state());
		this.nbrCases = Bytes.toBytes(builder.nbrCases());
		this.nbrDeaths = Bytes.toBytes(builder.nbrDeaths());
		this.fipCode = Bytes.toBytes(builder.fipCode());
		if(builder.county() != null) {
			this.county = Bytes.toBytes(builder.county());
		}
	}

	public byte[] getDtTracked() {
		return dtTracked;
	}

	public byte[] getState() {
		return state;
	}

	public byte[] getCounty() {
		return county;
	}

	public byte[] getFipCode() {
		return fipCode;
	}

	public byte[] getNbrCases() {
		return nbrCases;
	}

	public byte[] getNbrDeaths() {
		return nbrDeaths;
	}

	
	@Override
	public String toString() {
		return "Covid19Row [rowKey=" + new String(this.getRowKey()) + "]";
	}

	public static class Covid19RowBuilder implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private String dtTracked;
		private String state;
		private String county;
		private int fipCode;
		private long nbrCases;
		private long nbrDeaths;
		
		public Covid19RowBuilder() {}

		public String rowKey() {
			return this.state 
					+ this.dtTracked.replaceAll("/", "").replaceAll("-", "") 
					+ this.fipCode;
		}
		
		public String dtTracked() {
			return dtTracked;
		}
		
		public Covid19RowBuilder dtTracked(String dtTracked) {
			this.dtTracked = dtTracked;
			return this;
		}
		
		public String state() {
			return state;
		}
		
		public Covid19RowBuilder state(String state) {
			this.state = state;
			return this;
		}
		
		public String county() {
			return county;
		}
		
		public Covid19RowBuilder county(String county) {
			this.county = county;
			return this;
		}
		
		public int fipCode() {
			return fipCode;
		}
		
		public Covid19RowBuilder fipCode(int fipCode) {
			this.fipCode = fipCode;
			return this;
		}
		
		public long nbrCases() {
			return nbrCases;
		}
		
		public Covid19RowBuilder nbrCases(long nbrCases) {
			this.nbrCases = nbrCases;
			return this;
		}
		
		public long nbrDeaths() {
			return nbrDeaths;
		}
		
		public Covid19RowBuilder nbrDeaths(long nbrDeaths) {
			this.nbrDeaths = nbrDeaths;
			return this;
		}
		
		public Covid19Row build() {
			return new Covid19Row(this);
		}
		
		public DSCovid19RowSchema toDSCovid19RowSchema() {
			return new DSCovid19RowSchema(this);
		}
		
		@Override
		public String toString() {
			return "Covid19RowBuilder [dtTracked=" + dtTracked + ", state="
					+ state + ", county=" + county + ", fipCode=" + fipCode
					+ ", nbrCases=" + nbrCases + ", nbrDeaths=" + nbrDeaths
					+ "]";
		}
		
	}

}
