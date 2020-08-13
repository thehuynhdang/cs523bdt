package cs523.hbase;

import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import cs523.config.HBaseTable;
import cs523.model.Covid19Row;
import cs523.model.Covid19Row.Covid19RowBuilder;

public class FullCovid19DataTable extends HBaseTableDataPersistor<Covid19Row> {

	public FullCovid19DataTable(List<Covid19Row> covid19Rows) {
		super(covid19Rows);
	}

	@Override
	public Covid19Row updateRow(Covid19Row row, Result result) {
		Covid19RowBuilder rowBuilder = new Covid19RowBuilder();
		rowBuilder
		.county(Bytes.toString(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("county"))))
		.dtTracked(Bytes.toString(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("dtTracked"))))
		.fipCode(Bytes.toInt(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("fipCode"))))
		.state(Bytes.toString(
				result.getValue(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("state"))));
		
		rowBuilder.nbrCases(Bytes.toLong(row.getNbrCases()))
				  .nbrDeaths(Bytes.toLong(row.getNbrDeaths()));
		
		return rowBuilder.build();
	}

	//StateInfo: state, county, fipCode, dtTracked
	//NbrStatic: nbrCases, nbrDeaths
	@Override
	public Put createPut(Covid19Row row) {
		Put put = new Put(row.getRowKey());
		put.addColumn(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("state"), row.getState());
		put.addColumn(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("county"), row.getCounty());
		put.addColumn(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("fipCode"), row.getFipCode());
		put.addColumn(Bytes.toBytes(HBaseTable.FML_COL_NOUPDATE), Bytes.toBytes("dtTracked"), row.getDtTracked());
		put.addColumn(Bytes.toBytes(HBaseTable.FML_COL_FREQ_UPDATE), Bytes.toBytes("nbrCases"), row.getNbrCases());
		put.addColumn(Bytes.toBytes(HBaseTable.FML_COL_FREQ_UPDATE), Bytes.toBytes("nbrDeaths"), row.getNbrDeaths());
		
		return put;
	}

	@Override
	public TableName tableName() {
		return HBaseTable.FULL_TABLE_NAME;
	}
}
