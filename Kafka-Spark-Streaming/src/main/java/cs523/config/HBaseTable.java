package cs523.config;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.TableName;

public class HBaseTable {
	public static TableName FULL_TABLE_NAME = TableName.valueOf("USCovid19Cases");
	public static TableName CASESBYDATE_TABLE_NAME = TableName.valueOf("USCovid19CasesByDate");
	public static List<String> STATES = Arrays.asList("Washington","California","Virginia", "Texas", "New York");
	
	//columns: state, county, flipCode, dtTracked 
	public static String FML_COL_NOUPDATE = "StateInfo";
	//columns: nbrCases, nbrDeaths
	public static String FML_COL_FREQ_UPDATE = "NbrStatic";
}
