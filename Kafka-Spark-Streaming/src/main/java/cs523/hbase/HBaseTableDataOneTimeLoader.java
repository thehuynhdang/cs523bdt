package cs523.hbase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import cs523.config.HBaseTable;
import cs523.model.Covid19Row;
import cs523.model.Covid19Row.Covid19RowBuilder;
import cs523.model.SumCasesByDateRow;
import cs523.sparksql.SparkSQL;

public class HBaseTableDataOneTimeLoader {
	
	public static void main(String... args) throws IOException {
		
		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			
			System.out.print("TEST Creating table.... ");
			HTableDescriptor schemaTable = createUSCovid19CasesTable();
			if (admin.tableExists(schemaTable.getTableName())) {
				admin.disableTable(schemaTable.getTableName());
				admin.deleteTable(schemaTable.getTableName());
			}
			
			admin.createTable(schemaTable);
			
			schemaTable = createUSCovid19CasesByDateTable();
			if (admin.tableExists(schemaTable.getTableName())) {
				admin.disableTable(schemaTable.getTableName());
				admin.deleteTable(schemaTable.getTableName());
			}
			
			admin.createTable(schemaTable);
			connection.close();
			
			List<String> lines = 
					Files.lines(Paths.get("input/us_counties_covid19_daily_0811.csv"))
					.collect(Collectors.toList());
			
			List<Covid19Row> covid19Rows = lines.parallelStream().map(line -> {
				Covid19RowBuilder builder = new Covid19RowBuilder();
				
				//date,county,state,fips,cases,deaths
				String[] fields = line.split(",");
//				if(!HBaseTable.STATES.contains(fields[2])) {
//					return null;
//				}
				
				builder.dtTracked(fields[0])
				.county(fields[1])
				.state(fields[2])
				.fipCode(toInteger(fields[3]))
				.nbrCases(toInteger(fields[4]))
				.nbrDeaths(toInteger(fields[5]));
				
				return builder.build();
			}).filter(row -> row != null)
			  .collect(Collectors.toList());
			
			System.out.println("##########: FullCovid19DataTable started.");
			new FullCovid19DataTable(covid19Rows).persist();
			System.out.println("##########: FullCovid19DataTable ended.");
			
			System.out.println("##########: CasesByDateDatatable updating statics...");
    	    SparkSQL sparkSQL = new SparkSQL();
    	    List<SumCasesByDateRow> updates = sparkSQL.summarizeCasesByDate();
    	    List<Covid19Row> rows = updates.stream().map(row -> row.toCovid19Row()).collect(Collectors.toList());
	    	new CasesByDateDatatable(rows).persist();
    	    sparkSQL.closeSession();
    	    System.out.println("##########: CasesByDateDatatable end updates count " + updates.size());
    	    
			System.out.println(" Job Done!");
		}
	}
	
	private static int toInteger(String value) {
		return value != null && !value.isEmpty()? Integer.valueOf(value):0;
	}
	
	private static HTableDescriptor createUSCovid19CasesTable() {
		HTableDescriptor table = new HTableDescriptor(HBaseTable.FULL_TABLE_NAME);
		table.addFamily(new HColumnDescriptor(HBaseTable.FML_COL_NOUPDATE));
		table.addFamily(new HColumnDescriptor(HBaseTable.FML_COL_FREQ_UPDATE));
		return table;
	}
	
	private static HTableDescriptor createUSCovid19CasesByDateTable() {
		HTableDescriptor table = new HTableDescriptor(HBaseTable.CASESBYDATE_TABLE_NAME);
		table.addFamily(new HColumnDescriptor(HBaseTable.FML_COL_NOUPDATE));
		table.addFamily(new HColumnDescriptor(HBaseTable.FML_COL_FREQ_UPDATE));
		return table;
	}

}
