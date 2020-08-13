package cs523.sparksql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import cs523.config.HBaseTable;
import cs523.model.Covid19Row.Covid19RowBuilder;
import cs523.model.DSCovid19RowSchema;
import cs523.model.SumCasesByDateRow;

public class SparkSQL{
	static String DATE_FORMAT_STR = "yyyy-MM-dd";
	
	public static void main(String[] args) throws IOException {
		SparkSQL sparkSQL = new SparkSQL();
		
		// test
		sparkSQL.selectAllTest();
		
		//sparkSQL.showSummaryCasesByDateTest();
		
		//sparkSQL.summarizeCasesByDate();
		
		sparkSQL.closeSession();
	}
	
	private SparkSession sparkSession;
	public SparkSQL() throws IOException {
		this.sparkSession = SparkSession.builder()
				.appName("SparkSQL").master("local")
				.getOrCreate();
		
		//query and create DataFrame/DataSet for Spark SQL
		this.createUSCovid19CasesDataFrame();
	}
	
	public void selectAllTest() {
		String sql = "SELECT rowKey,to_date(dtTracked, 'yyyy-MM-dd') as dtTracked,state,county,fipCode,nbrCases,nbrDeaths "
				   + "FROM " + HBaseTable.FULL_TABLE_NAME.getNameAsString()
				   + " WHERE fipCode=42003 AND dtTracked='2020-08-11'"
				   + " ORDER BY dtTracked DESC";
		
		Dataset<Row> data = sparkSession.sql(sql);
		
		System.out.println("COUNT " + data.count());
		data.show(100);
	}
	
	public void showSummaryCasesByDateTest() {
		String sql = "SELECT rowKey,dtTracked,state,nbrCases,nbrDeaths"
				  + " FROM " + HBaseTable.CASESBYDATE_TABLE_NAME.getNameAsString()
				  + " ORDER BY dtTracked DESC, state";
		
		Dataset<Row> data = sparkSession.sql(sql);
		data.show(30);
		
		//System.out.println("COUNT " + data.count());
	}
	
	public void showQueryCasesByDateTest() {
		String sql = "SELECT to_date(dtTracked, 'yyyy-MM-dd') as dtTracked,"
				+ " state, sum(nbrCases) as nbrCases, sum(nbrDeaths) as nbrDeaths"
				+ " FROM " + HBaseTable.FULL_TABLE_NAME.getNameAsString()
				+ " GROUP BY dtTracked,state"
				+ " ORDER BY dtTracked DESC, state";
		
		Dataset<Row> data = sparkSession.sql(sql);
		data.show(30);
		
		//System.out.println("COUNT " + data.count());
	}
	
	public List<SumCasesByDateRow> summarizeCasesByDate() {
		String sql = "SELECT dtTracked,"
				  + " state, sum(nbrCases) as nbrCases, sum(nbrDeaths) as nbrDeaths"
				  + " FROM " + HBaseTable.FULL_TABLE_NAME.getNameAsString()
				  + " GROUP BY dtTracked,state";
		
		Dataset<Row> data = sparkSession.sql(sql);
		List<SumCasesByDateRow> list = data.as(
				Encoders.bean(SumCasesByDateRow.class)).collectAsList();
		
		System.out.println("##########: Row Count " + list.size());
		return list;
	}
	
	private void createUSCovid19CasesDataFrame() throws IOException {
		Configuration config = HBaseConfiguration.create();
		Scan s = new Scan();
		try (Connection connection = ConnectionFactory.createConnection(config);
				Table dataTable = connection.getTable(HBaseTable.FULL_TABLE_NAME);
				Admin admin = connection.getAdmin();
				ResultScanner scanner = dataTable.getScanner(s)) {
			
			List<DSCovid19RowSchema> rows = new ArrayList<>();
			Result result = scanner.next();
			while(result != null) {
				rows.add(createCovid19RowBuilder(result).toDSCovid19RowSchema());
				result = scanner.next();
			}
			
			sparkSession.createDataFrame(rows, DSCovid19RowSchema.class)
			.createOrReplaceTempView(HBaseTable.FULL_TABLE_NAME.getNameAsString()); 
			
			connection.close();
			System.out.println("DataFrame: Scan HBase Table Done!");
		}
	}
	
	private Covid19RowBuilder createCovid19RowBuilder(Result result) {
		Covid19RowBuilder rowBuilder = new Covid19RowBuilder();
		rowBuilder
		.county(Bytes.toString(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("county"))))
		.dtTracked(Bytes.toString(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("dtTracked"))))
		.fipCode(Bytes.toInt(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("fipCode"))))
		.state(Bytes.toString(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("state"))))
		.nbrCases(Bytes.toLong(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_FREQ_UPDATE), Bytes.toBytes("nbrCases"))))
		.nbrDeaths(Bytes.toLong(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_FREQ_UPDATE), Bytes.toBytes("nbrDeaths"))));
		
		return rowBuilder;
	}
	
	public void closeSession() {
		this.sparkSession.close();
	}
}
