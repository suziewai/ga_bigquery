package au.com.vocus.bq;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
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

public class GaDailyThread extends AbstractGaThread {
	
	private final String TABLE_PREFIX = "ga_sessions_";
	private int maxPageSize = 50000;
	//private int maxPageSize = 50;
		
	private String startExportDate = "";
	private String stopExportDate = "";
	
	private LinkedHashMap<String, Job> jobList = new LinkedHashMap<String, Job>();
	
	public GaDailyThread(Dataset dataset) {
		this.dataset = dataset;
	}
	
	public void setStartExportDate(String value) {
		this.startExportDate = value;
	}
	
	public void setStopExportDate(String value) {
		this.stopExportDate = value;
	}
		
	@Override
	public void run() {
		
		this.dsId = dataset.getDatasetId().getDataset();
		this.sql = buildQueryString(csvWriter.getColumns());
		
		System.out.println(Calendar.getInstance().getTime() + " - GaDailyThread " + dsId + " started...");
		
		startExportDate = prop.getLastUpdate(dsId);
		stopExportDate = prop.getFormatter().format(new Date());
		
		String tableId = getTableId(startExportDate);
		while(tableId.compareTo(stopExportDate) < 0) {
			Job job = submitJob(TABLE_PREFIX + tableId);
			jobList.put(tableId, job);
			tableId = getTableId(tableId);
		}
 
		for(String key : jobList.keySet()) {
			try {
				if(getData(key, jobList.get(key))) {
					prop.setLastUpdate(dsId, key);
				}
			} catch (Exception e) {
				System.out.println("Error occurred in querying job " + jobList.get(key).getJobId().getJob());
				e.printStackTrace();
			}
		}
   		
   		System.out.println(Calendar.getInstance().getTime() + " - GaDailyThread " + dsId + " completed...");
	}

	private boolean getData(String tableId, Job queryJob) throws InterruptedException, TimeoutException {
		
		System.out.println(dsId + ":" + queryJob.getJobId() + ":" + queryJob.isDone() + ":" + queryJob.getStatus());
		
		/* Wait for the query to complete. */
		queryJob = queryJob.waitFor();

		/* Check for errors */
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
			return false;
		}

		/* Get the results. */
		QueryResponse response = bigquery.getQueryResults(queryJob.getJobId(), QueryResultsOption.pageSize(maxPageSize));
		QueryResult result = response.getResult();

		String filename = dsId + "_" + tableId + "_daily.csv";
		csvWriter.createFolder("daily");
	    csvWriter.createFile(filename, false, ",", true);
		
		//for (List<FieldValue> row : result.getValues()) {
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
		try {
			csvWriter.moveCompletedFile(filename, "completed");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;
	}
	
	private Job submitJob(String tableId) {
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(getQueryString(tableId)).build();

		/* Create a job ID so that we can safely retry. */
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig)
												.setJobId(jobId)
												.build());
		
		return queryJob;
	}
	
	private String getTableId(String lastUpdate) {
		Calendar nextDate = Calendar.getInstance();
		try {
			nextDate.setTime(prop.getFormatter().parse(lastUpdate));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		nextDate.add(Calendar.DATE, 1);
		return prop.getFormatter().format(nextDate.getTime());
	}

}
