package au.com.vocus.bq;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.Dataset;

public class GaIntradayThread extends AbstractGaThread {

	//private boolean LIVE_MODE = false;
	public static final String TABLE_PREFIX = "ga_sessions_intraday_";
	private int maxPageSize = 50000;
		
	private String intraDay;
	
	public GaIntradayThread(Dataset dataset) {
		this.dataset = dataset;
	}
	
	public GaIntradayThread(Dataset dataset, String today) {
		this(dataset);
		this.intraDay = today;
	}
	
	public void setIntraDate(String value) {
		this.intraDay = value;
	}
	
	@Override
	public void run() {
		
		this.dsId = dataset.getDatasetId().getDataset();
		this.sql = buildQueryString(csvWriter.getColumns());
		
		System.out.println(Calendar.getInstance().getTime() + " - GaIntradayThread " + dsId + " started...");
			
		intraDay = prop.getFormatter().format(new Date());
		Job job = submitJob(TABLE_PREFIX + intraDay);
   		   		
   		try {
			getData(job);
		} catch (Exception e) {
			System.out.println("Error occurred in querying job " + job.getJobId().getJob());
			e.printStackTrace();
		}
   		   		
   		System.out.println(Calendar.getInstance().getTime() + " - GaIntradayThread " + dsId + " completed...");
	}

	private void getData(Job queryJob) throws InterruptedException, TimeoutException {
		
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

		csvWriter.createFolder("intraday");
		csvWriter.createFile(dsId + "_intraday.csv", false, ",", true);
		
		for (List<FieldValue> row : result.iterateAll()) {
			boolean isFirstColumn = true;
			for (FieldValue val : row) {
				for(Object value : getFieldValue(val, new ArrayList<Object>())) {
					csvWriter.writeField(value, isFirstColumn);
					isFirstColumn = false;
				}
			}
		}
		
		csvWriter.closeFile();

	}
	
	private Job submitJob(String tableId) {
			
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(getQueryString(tableId)).build();

		/* Create a job ID so that we can safely retry. */
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
		
		return queryJob;
	}
	
}
