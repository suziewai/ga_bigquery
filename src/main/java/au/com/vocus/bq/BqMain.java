package au.com.vocus.bq;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;


public class BqMain {
	
	private static final String OPTION_LIVE_MODE = "-lm";
	private static final String OPTION_START_DATE = "-sd";
	private static final String OPTION_END_DATE = "-ed";
	private static final String OPTION_NEW_FILE = "-nf";
	private static final String OPTION_PAGE_SIZE = "-ps";
	private static final String OPTION_DS_LIST = "-dl";
	private static final String OPTION_MODE = "-mode";
		
	private static final String PROJECT_ID = "vocusgroup";
	private static Hashtable<String, String> arguments;
	
	private static List<Thread> workers = new ArrayList<Thread>();
	
	private static final class APP_MODE {
		public static final String HISTORY = "history";
		public static final String DAILY = "daily";
		public static final String INTRADAY = "intraday";
	}
	
	public static void main(String[] args) {

		parseArgList(args);
		Property prop = new Property(arguments.get(OPTION_MODE));
		
		// Instantiates a client
		//BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    Page<Dataset> dsList = bigquery.listDatasets(PROJECT_ID);
	    Iterable<Dataset> dss = arguments.get(OPTION_DS_LIST) == null ? dsList.iterateAll() : getDsList(bigquery);
	   
	    for(Dataset ds : dss) {
	    	if(prop.getDisabled(ds.getDatasetId().getDataset()))
	    		continue;
	    	
	    	AbstractGaThread gaThread = getGaThread(ds);
	    	gaThread.setProperty(prop);
	    	Thread t = new Thread(gaThread);
	    	workers.add(t);
	    	t.start();
	    }
	    
	    for(Thread t : workers) {
	    	try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    //runTest(dsList);
	    prop.save();
	    System.out.println("Main process completed....");
	}
	
	private static AbstractGaThread getGaThread(Dataset ds) {
		switch(arguments.get(OPTION_MODE)) {
		case APP_MODE.INTRADAY:
			return new GaIntradayThread(ds);
		case APP_MODE.DAILY:
			return new GaDailyThread(ds);
		case APP_MODE.HISTORY:
		default:
			GaSessionThread gaThread = new GaSessionThread(ds);
	    	
    		if(arguments.get(OPTION_LIVE_MODE) != null)
    			gaThread.setLiveMode(Boolean.valueOf(arguments.get(OPTION_LIVE_MODE)));
    		if(arguments.get(OPTION_START_DATE) != null)
    			gaThread.setStartExportDate(arguments.get(OPTION_START_DATE));
    		if(arguments.get(OPTION_END_DATE) != null)
    			gaThread.setStopExportDate(arguments.get(OPTION_END_DATE));
    		if(arguments.get(OPTION_NEW_FILE) != null)
    			gaThread.setNewFile(Boolean.valueOf(arguments.get(OPTION_NEW_FILE)));
    		if(arguments.get(OPTION_PAGE_SIZE) != null)
    			gaThread.setPageSize(Integer.parseInt(arguments.get(OPTION_PAGE_SIZE)));
    		return gaThread;
		}
	}
	
	private static void parseArgList(String[] args) {
		arguments = new Hashtable<String, String>();
		for(int i = 0; i < args.length; i++) {
			if(!args[i].startsWith("-"))
				continue;
			if(i+1 < args.length)
				arguments.put(args[i], args[i+1]);
		}
	}
	
	private static Iterable<Dataset> getDsList(BigQuery bigquery) {
		ArrayList<Dataset> dss = new ArrayList<Dataset>();
		String[] dsList = arguments.get(OPTION_DS_LIST).split(",");
		for(int i=0; i<dsList.length; i++) {
			dss.add(bigquery.getDataset(dsList[i]));
		}
		return dss;
	}
	
	/*
	private static void runTest(Page<Dataset> ds) {
		Test.printQueryOnly();
		
		for(Dataset dataset : ds.getValues()) {
		try {
    		Test.getTableSchema(dataset);
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
		break;
		}
		
	}
	*/
}
