package au.com.vocus.bq;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.BigQuery.DatasetField;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.DatasetOption;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.BigQuery.TableField;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQuery.TableOption;

public class Test{
	
	private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	private static PrintWriter pw;
	
	static {
		try {
			pw = new PrintWriter(new File("output.log"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void runTesting() throws FileNotFoundException, InterruptedException, TimeoutException {
		String query = "SELECT visitorId, visitNumber, visitId, visitStartTime, date, totals.visits, totals.hits, totals.pageviews, totals.timeOnSite, totals.bounces, totals.transactions, totals.transactionRevenue, totals.newVisits, totals.screenviews, totals.uniqueScreenviews, totals.timeOnScreen, totals.totalTransactionRevenue, trafficSource.referralPath, trafficSource.campaign, trafficSource.source, trafficSource.medium, trafficSource.keyword, trafficSource.adContent, trafficSource.adwordsClickInfo.campaignId, trafficSource.adwordsClickInfo.adGroupId, trafficSource.adwordsClickInfo.creativeId, trafficSource.adwordsClickInfo.criteriaId, trafficSource.adwordsClickInfo.page, trafficSource.adwordsClickInfo.slot, trafficSource.adwordsClickInfo.criteriaParameters, trafficSource.adwordsClickInfo.gclId, trafficSource.adwordsClickInfo.customerId, trafficSource.adwordsClickInfo.adNetworkType, trafficSource.adwordsClickInfo.targetingCriteria.boomUserlistId, trafficSource.adwordsClickInfo.isVideoAd, trafficSource.isTrueDirect, trafficSource.campaignCode, device.browser, device.operatingSystem, device.isMobile, device.mobileDeviceBranding, device.mobileDeviceModel, device.mobileDeviceMarketingName, device.language, device.deviceCategory, geoNetwork.continent, geoNetwork.subContinent, geoNetwork.country, geoNetwork.region, geoNetwork.metro, geoNetwork.city, geoNetwork.cityId, geoNetwork.networkDomain, geoNetwork.latitude, geoNetwork.longitude, geoNetwork.networkLocation, hits.customDimensions.index, hits.customDimensions.value, hits.hitNumber, hits.time, hits.hour, hits.minute, hits.isEntrance, hits.isExit, hits.referer, hits.page.pagePath, hits.page.hostname, hits.page.pageTitle, hits.page.searchKeyword, hits.page.searchCategory, hits.transaction.transactionId, hits.transaction.transactionRevenue, hits.transaction.transactionTax, hits.transaction.transactionShipping, hits.transaction.affiliation, hits.transaction.currencyCode, hits.transaction.localTransactionRevenue, hits.transaction.localTransactionTax, hits.transaction.localTransactionShipping, hits.transaction.transactionCoupon, hits.item.transactionId, hits.item.productName, hits.item.productCategory, hits.item.itemQuantity, hits.item.itemRevenue, hits.item.currencyCode, hits.item.localItemRevenue, hits.contentInfo.contentDescription, hits.appInfo.name, hits.appInfo.version, hits.appInfo.id, hits.appInfo.installerId, hits.appInfo.appInstallerId, hits.appInfo.appName, hits.appInfo.appVersion, hits.appInfo.appId, hits.appInfo.screenName, hits.appInfo.landingScreenName, hits.appInfo.exitScreenName, hits.appInfo.screenDepth, hits.eventInfo.eventCategory, hits.eventInfo.eventAction, hits.eventInfo.eventLabel, hits.eventInfo.eventValue, hits.product.productSKU, hits.product.v2ProductName, hits.product.v2ProductCategory, hits.product.productVariant, hits.product.productBrand, hits.product.productRevenue, hits.product.localProductRevenue, hits.product.productPrice, hits.product.localProductPrice, hits.product.productQuantity, hits.product.productRefundAmount, hits.product.localProductRefundAmount, hits.product.isImpression, hits.product.isClick, hits.product.productListName, hits.product.productListPosition, hits.refund.refundAmount, hits.refund.localRefundAmount, hits.eCommerceAction.action_type, hits.eCommerceAction.step, hits.eCommerceAction.option, hits.customVariables.index, hits.customVariables.customVarName, hits.customVariables.customVarValue, hits.type, hits.social.socialInteractionNetwork, hits.social.socialInteractionAction, hits.social.socialInteractions, hits.social.socialInteractionTarget, hits.social.socialNetwork, hits.social.uniqueSocialInteractions, hits.social.hasSocialSourceReferral, hits.social.socialInteractionNetworkAction, hits.contentGroup.contentGroup1, hits.contentGroup.contentGroup2, hits.contentGroup.contentGroup3, hits.contentGroup.contentGroup4, hits.contentGroup.contentGroup5, hits.contentGroup.previousContentGroup1, hits.contentGroup.previousContentGroup2, hits.contentGroup.previousContentGroup3, hits.contentGroup.previousContentGroup4, hits.contentGroup.previousContentGroup5, hits.contentGroup.contentGroupUniqueViews1, hits.contentGroup.contentGroupUniqueViews2, hits.contentGroup.contentGroupUniqueViews3, hits.contentGroup.contentGroupUniqueViews4, hits.contentGroup.contentGroupUniqueViews5, hits.dataSource, fullVisitorId, userId, channelGrouping, socialEngagementType, hits.promotion.promoId, hits.promotion.promoName, hits.promotion.promoCreative, hits.promotion.promoPosition, hits.promotionActionInfo.promoIsView, hits.promotionActionInfo.promoIsClick, hits.customMetrics.index, hits.customMetrics.value, customDimensions.index, customDimensions.value, hits.product.customDimensions.index, hits.product.customDimensions.value, hits.product.customMetrics.index, hits.product.customMetrics.value ";
		query += "FROM [74070648.ga_sessions_20171012] LIMIT 10";
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

		/* Create a job ID so that we can safely retry. */
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
		
		/* Wait for the query to complete. */
		queryJob = queryJob.waitFor();

		/* Check for errors */
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
		    throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		/* Get the results. */
		QueryResponse response = bigquery.getQueryResults(queryJob.getJobId(), QueryResultsOption.pageSize(10));
		QueryResult result = response.getResult();
	}
		
	public static void printQueryOnly() {
		
		GaSessionThread gaThread = new GaSessionThread(null);
		System.out.println(gaThread.getQuery());
	}
	
	public static void getTableSchema(Dataset ds) throws FileNotFoundException {
		
   		Page<Table> tableList = ds.list(TableListOption.pageSize(300));
   		
		for(Table table : tableList.getValues()) {
			Table schema = bigquery.getTable(table.getTableId()
					//, TableOption.fields(TableField.TYPE)
					//, TableOption.fields(TableField.CREATION_TIME)
					//, TableOption.fields(TableField.EXPIRATION_TIME)
					, TableOption.fields(TableField.SCHEMA)
					);
			TableDefinition td = schema.getDefinition();
			System.out.println(table.getTableId());
			printColumnName(td.getSchema().getFields(), null);
			System.out.println(td.getSchema().getFields().size());
			
			break;
		}
	}
	
	public static void querySome(BigQuery bigquery) throws InterruptedException, TimeoutException {
		
		String queryString = "SELECT visitNumber, totals.visits, totals.hits FROM [111460146.ga_sessions_20160328]";
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString).build();

		// Create a job ID so that we can safely retry.
		  JobId jobId = JobId.of(UUID.randomUUID().toString());
		  Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		  // Wait for the query to complete.
		  queryJob = queryJob.waitFor();

		  // Check for errors
		  if (queryJob == null) {
		    throw new RuntimeException("Job no longer exists");
		  } else if (queryJob.getStatus().getError() != null) {
		    // You can also look at queryJob.getStatus().getExecutionErrors() for all
		    // errors, not just the latest one.
		    throw new RuntimeException(queryJob.getStatus().getError().toString());
		  }

		  // Get the results.
		  QueryResponse response = bigquery.getQueryResults(jobId);
		  QueryResult result = response.getResult();

		  // Print all pages of the results.
		  while (result != null) {
		    for (List<FieldValue> row : result.iterateAll()) {
		      for (FieldValue val : row) {
		        System.out.printf("%s,", val.toString());
		        printFieldValue(null, null, val);
		      }
		      System.out.printf("\n");
		    }

		    result = result.getNextPage();
		  }
	}
	
	public static void getEverything(BigQuery bigquery) {
		try {
			pw = new PrintWriter(new File("test.txt"));
		} catch(Exception e) {
			e.printStackTrace();
		}
		    
		Page<Dataset> page = bigquery.listDatasets(DatasetListOption.all());
		
		System.out.println("ga_sessions......");
		for(Dataset ds : page.iterateAll()) {
			
			try {
				Page<Table> tables = ds.list(TableListOption.pageSize(1000000));
				for(Table table : tables.iterateAll()) {
									
					Page<List<FieldValue>> value = table.list(TableDataListOption.startIndex(0));
					for(List<FieldValue> values : value.iterateAll()) {
						//System.out.println("First Column........");
						pw.println("Record starts ....");
						for(FieldValue field : values) {
							printFieldValue(ds.getDatasetId(), table.getTableId(), field);
						}
					}
				}
			} catch(Exception e) {
				e.printStackTrace();
				continue;
			}
			
		}
	}
	
	public static void getDsDescriptions() {
		Page<Dataset> page = bigquery.listDatasets(DatasetListOption.all());
		for(Dataset ds : page.iterateAll()) {
			Dataset dsd = bigquery.getDataset(ds.getDatasetId(), DatasetOption.fields(DatasetField.DESCRIPTION
					, DatasetField.FRIENDLY_NAME
					, DatasetField.LABELS));
			pw.printf("Id: %s - \tDesc: %s\r\n", dsd.getDatasetId(), dsd.getDescription());
			System.out.printf("Id: %s - \tDesc: %s\r\n", dsd.getDatasetId(), dsd.getDescription());
		}
	}
	
	private static void printColumnName(List<Field> columns, String parent) {

		String newParent = parent == null ? "" : parent + ".";
		for(Field column : columns) {
			if(column.getFields() == null) {
//				pw.print("Column : ");				
					System.out.println(newParent+column.getName());
			} else {
				printColumnName(column.getFields(), newParent+column.getName());
			}
		}
	}
	
	private static void printFieldValue(DatasetId dsId, TableId tableId, FieldValue fieldValue) {
		
		if(FieldValue.Attribute.RECORD.equals(fieldValue.getAttribute())
				|| FieldValue.Attribute.REPEATED.equals(fieldValue.getAttribute())) {
			for(FieldValue field : fieldValue.getRecordValue()) {
				printFieldValue(dsId, tableId, field);
			}
		}
		else {
			System.out.printf("DS %s : Table %s : Field : %s \r\n", "", "", fieldValue.getValue());
			//pw.printf("DS %s : Table %s : Field : %s \r\n", dsId, tableId, fieldValue.getValue());
		}
	}
}
