package cs523.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import cs523.model.HRowKey;

public abstract class HBaseTableDataPersistor<T extends HRowKey> {
	
	private List<T> covid19Rows;
	public HBaseTableDataPersistor(List<T> covid19Rows) {
		this.covid19Rows = covid19Rows;
	}
	
	public void persist() throws IOException {
		System.out.println("Persist Started!");
		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			
			Table dataTable = connection.getTable(tableName());
			covid19Rows.parallelStream().forEach(row -> {
				System.out.println(row);
				
				try {
					Result result = dataTable.get(new Get(row.getRowKey()));
					
					//existed and merged
					if(result != null && !result.isEmpty()) {
						row = updateRow(row, result);
					}
					
					Put put = createPut(row);
					dataTable.put(put);
					
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			});
			
			connection.close();
			System.out.println("Persist Done!");
		}
	}
	
	public abstract T updateRow(T row, Result result);
	public abstract Put createPut(T row);
	public abstract TableName tableName();

}
