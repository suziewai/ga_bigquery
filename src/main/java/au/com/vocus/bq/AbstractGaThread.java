package au.com.vocus.bq;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.FieldValue;

public abstract class AbstractGaThread implements Runnable{

	protected final String TABLE_ID = "##TABLE_ID##";
	protected String dsId = "##DS_ID##";
	protected String sql;
	protected List<String> repeatedColumns = new ArrayList<String>();
	
	protected BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	protected GaSessionCsvWriter csvWriter = new GaSessionCsvWriter();
	protected Dataset dataset;
	
	protected Property prop;
	
	@Override
	public abstract void run();
	
	public void setProperty(Property value) {
		this.prop = value;
	}
	
	public String getQuery() {
		sql = sql == null ? buildQueryString(csvWriter.getColumns()) : sql;
		return sql;
	}
	
	protected List<Object> getFieldValue(FieldValue fieldValue, List<Object> recordValues) {
		if(FieldValue.Attribute.RECORD.equals(fieldValue.getAttribute())
				|| FieldValue.Attribute.REPEATED.equals(fieldValue.getAttribute())) {
			for(FieldValue field : fieldValue.getRecordValue()) {
				getFieldValue(field, recordValues);
			}
		}
		else {
			recordValues.add((fieldValue.getValue()));
		}
		return recordValues;
	}
			
	protected String getQueryString(String tableId) {
		return sql.replaceAll(TABLE_ID, tableId);
	}
		
	protected String buildQueryString(List<String> columns) {
		StringBuffer strBuffer = new StringBuffer ("SELECT ");
		
		boolean isFirstColumn = true;
		for(String column : columns) {
			if(isFirstColumn) {
				strBuffer.append(column);
				isFirstColumn = false;
			}
			else {
				if("visitStartTime".equals(column))
					strBuffer.append(", STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(" + column + "), \"%Y-%m-%d %H:%M:%S\") AS startTime");
					//strBuffer.append(", STRFTIME_UTC_USEC(SEC_TO_TIMESTAMP(" + column + "), \"%Y-%m-%d %H:%M:%S %Z\") AS startTime");
				else
					strBuffer.append(", " + column);
			}
		}
		
		strBuffer.append(" FROM " + getTable("[" + dsId + "." + TABLE_ID + "]"));
		return strBuffer.toString();
	}
	
	protected String getTable(String tableName) {
		for(String col : getRepeatedColumns()) {
			tableName = "FLATTEN(" + tableName + "," + col + ")";
		}
		return tableName;
	}
	
	protected List<String> getRepeatedColumns() {
		if(repeatedColumns.size() == 0) {
			repeatedColumns.add("customDimensions");
			repeatedColumns.add("hits.customVariables");
			repeatedColumns.add("hits.customDimensions");
			repeatedColumns.add("hits.customMetrics");
			repeatedColumns.add("hits.product.customDimensions");
			repeatedColumns.add("hits.product.customMetrics");
			repeatedColumns.add("hits.promotion");
		}
		return repeatedColumns;
	}
}
