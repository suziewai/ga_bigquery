package au.com.vocus.bq;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.Dataset;

public class GaSessionThread extends AbstractGaThread {
	
	private final String TABLE_PREFIX = "ga_sessions_";
		
	private boolean LIVE_MODE = false;
	private String startExportDate = "";
	private String stopExportDate = "";
	private boolean newFile = false;
	private int maxPageSize = 5000;
	
	public GaSessionThread(Dataset dataset) {
		this.dataset = dataset;
	}
	
	public void setLiveMode(boolean value) {
		this.LIVE_MODE = value;
	}
	
	public void setStartExportDate(String value) {
		this.startExportDate = value;
	}
	
	public void setStopExportDate(String value) {
		this.stopExportDate = value;
	}
	
	public void setNewFile(boolean value) {
		this.newFile = value;
	}
	
	public void setPageSize(int value) {
		this.maxPageSize = value;
	}
	
	@Override
	public void run() {
		
		this.dsId = dataset.getDatasetId().getDataset();
		this.sql = buildQueryString(csvWriter.getColumns());
		
		System.out.println(Calendar.getInstance().getTime() + " - GaSessionThread " + dsId + " started...");
			
		Page<Table> pageList = dataset.list(TableListOption.pageSize(maxPageSize));
		Iterable<Table> tableList = LIVE_MODE ? pageList.iterateAll() : pageList.getValues();
		ArrayList<Job> jobList = new ArrayList<Job>();

		for(Table table : tableList) {
			Job job = submitJob(table.getTableId().getTable());
			if(job != null) {
				jobList.add(job);
				try {
					Thread.sleep(150);
				} catch (InterruptedException e) {				
					e.printStackTrace();
				}
			}
   		}
   		
		/* changing to one file per dataset
		csvWriter.createFolder(dsId);*/
		if(newFile) {
			csvWriter.createFile(dsId + ".csv", true, ",", true);
			csvWriter.closeFile();
		}
		
   		for(Job job : jobList) {
   			try {
				getData(job);
			} catch (Exception e) {
				System.out.println("Error occurred in querying job " + job.getJobId().getJob());
				e.printStackTrace();
			}
   		}
   		
   		System.out.println(Calendar.getInstance().getTime() + " - GaSessionThread " + dsId + " completed...");
	}

	private void getData(Job queryJob) throws InterruptedException, TimeoutException {
		
		System.out.println(dsId + ":" + queryJob.getJobId() + ":" + queryJob.isDone() + ":" + queryJob.getStatus());
		
		/* Wait for the query to complete. */
		queryJob = queryJob.waitFor();

		/* Check for errors */
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
		    throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		/* Get the results. */
		QueryResponse response = bigquery.getQueryResults(queryJob.getJobId(), QueryResultsOption.pageSize(maxPageSize));
		QueryResult result = response.getResult();

		String filename = dsId + ".csv";
		/* changing to one file per dataset
	    csvWriter.createFile(tableId + ".csv", true, ",", true); */
		csvWriter.appendFile(filename, ",", true);
		
		Iterable<List<FieldValue>> valueList = LIVE_MODE ? result.iterateAll() : result.getValues();
		for (List<FieldValue> row : valueList) {
			boolean isFirstColumn = true;
			for (FieldValue val : row) {
				for(Object value : getFieldValue(val, new ArrayList<Object>())) {
					csvWriter.writeField(value, isFirstColumn);
					isFirstColumn = false;
				}
			}
		}
		
		csvWriter.closeFile();
		/*
		try {
			csvWriter.moveCompletedFile(filename, "completed");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	private Job submitJob(String tableId) {
		if(!tableId.contains(TABLE_PREFIX))
			return null;
		if(tableId.contains("intraday"))
			return null;
		if(tableId.substring(TABLE_PREFIX.length()).compareTo(startExportDate) < 0)
			return null;
		if(tableId.substring(TABLE_PREFIX.length()).compareTo(stopExportDate) > 0)
			return null;
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(getQueryString(tableId))
																			.setPriority(QueryJobConfiguration
																			.Priority.BATCH)
																			.build();

		/* Create a job ID so that we can safely retry. */
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig)
												.setJobId(jobId)
												.build());
		
		return queryJob;
	}
	
	

}
