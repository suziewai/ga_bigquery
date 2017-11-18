package au.com.vocus.bq;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class GaSessionCsvWriter {

	private String PATH_PREFIX = "/data/app/bq";
	private List<String> columns;
	private PrintWriter pw = null;
	private String delimiter = "";
	private boolean withQuote;
	private String filepath;
	
	public GaSessionCsvWriter() {
		columns = new ArrayList<String>();
		columns.add("fullVisitorId");
		
		columns.add("visitNumber");
		columns.add("visitId");
		columns.add("visitStartTime");
		columns.add("date");
		columns.add("totals.visits");
		columns.add("totals.hits");
		columns.add("totals.pageviews");
		columns.add("totals.timeOnSite");
		columns.add("totals.bounces");
		columns.add("totals.transactions");
		columns.add("totals.newVisits");
		columns.add("totals.screenviews");
		columns.add("totals.uniqueScreenviews");
		columns.add("totals.timeOnScreen");
		columns.add("totals.totalTransactionRevenue");
		
		columns.add("trafficSource.referralPath");
		columns.add("trafficSource.campaign");
		columns.add("trafficSource.source");
		columns.add("trafficSource.medium");
		columns.add("trafficSource.keyword");
		columns.add("trafficSource.adContent");
		columns.add("trafficSource.adwordsClickInfo.campaignId");
		columns.add("trafficSource.adwordsClickInfo.adGroupId");
		columns.add("trafficSource.adwordsClickInfo.creativeId");
		columns.add("trafficSource.adwordsClickInfo.criteriaId");
		columns.add("trafficSource.adwordsClickInfo.page");
		columns.add("trafficSource.adwordsClickInfo.slot");
		columns.add("trafficSource.adwordsClickInfo.criteriaParameters");
		columns.add("trafficSource.adwordsClickInfo.gclId");
		columns.add("trafficSource.adwordsClickInfo.customerId");
		columns.add("trafficSource.adwordsClickInfo.adNetworkType");
		columns.add("trafficSource.adwordsClickInfo.targetingCriteria.boomUserlistId");
		columns.add("trafficSource.adwordsClickInfo.isVideoAd");
		columns.add("trafficSource.isTrueDirect");
		columns.add("trafficSource.campaignCode");
		columns.add("device.browser");
		columns.add("device.operatingSystem");
		columns.add("device.isMobile");
		columns.add("device.mobileDeviceBranding");
		columns.add("device.mobileDeviceModel");		
		columns.add("device.mobileDeviceMarketingName");
		columns.add("device.language");
		columns.add("device.deviceCategory");
		columns.add("geoNetwork.continent");
		columns.add("geoNetwork.subContinent");
		columns.add("geoNetwork.country");
		columns.add("geoNetwork.region");
		columns.add("geoNetwork.metro");
		columns.add("geoNetwork.city");
		columns.add("geoNetwork.cityId");
		columns.add("geoNetwork.networkDomain");
		columns.add("geoNetwork.latitude");
		columns.add("geoNetwork.longitude");
		columns.add("geoNetwork.networkLocation");

		columns.add("customDimensions.index");
		columns.add("customDimensions.value");
		
		columns.add("hits.hitNumber");
		columns.add("hits.time");
		columns.add("hits.hour");
		columns.add("hits.minute");		
		columns.add("hits.isEntrance");
		columns.add("hits.isExit");
		columns.add("hits.referer");
		columns.add("hits.page.pagePath");
		columns.add("hits.page.hostname");
		columns.add("hits.page.pageTitle");
		columns.add("hits.page.searchKeyword");
		columns.add("hits.page.searchCategory");
		columns.add("hits.transaction.transactionId");
		columns.add("hits.transaction.transactionRevenue");
		columns.add("hits.transaction.transactionTax");
		columns.add("hits.transaction.transactionShipping");
		columns.add("hits.transaction.affiliation");
		columns.add("hits.transaction.currencyCode");
		columns.add("hits.transaction.localTransactionRevenue");
		columns.add("hits.transaction.localTransactionTax");
		columns.add("hits.transaction.localTransactionShipping");
		columns.add("hits.transaction.transactionCoupon");
		columns.add("hits.item.transactionId");
		columns.add("hits.item.productName");
		columns.add("hits.item.productCategory");
		columns.add("hits.item.itemQuantity");
		columns.add("hits.item.itemRevenue");
		columns.add("hits.item.currencyCode");
		columns.add("hits.item.localItemRevenue");
		columns.add("hits.contentInfo.contentDescription");
		columns.add("hits.appInfo.name");
		columns.add("hits.appInfo.version");
		columns.add("hits.appInfo.id");
		columns.add("hits.appInfo.installerId");
		columns.add("hits.appInfo.appInstallerId");
		columns.add("hits.appInfo.appName");
		columns.add("hits.appInfo.appVersion");
		columns.add("hits.appInfo.appId");
		columns.add("hits.appInfo.screenName");
		columns.add("hits.appInfo.landingScreenName");
		columns.add("hits.appInfo.exitScreenName");
		columns.add("hits.appInfo.screenDepth");
		
		columns.add("hits.eventInfo.eventCategory");
		columns.add("hits.eventInfo.eventAction");
		columns.add("hits.eventInfo.eventLabel");
		columns.add("hits.eventInfo.eventValue");
		
		columns.add("hits.product.productSKU");
		columns.add("hits.product.v2ProductName");
		columns.add("hits.product.v2ProductCategory");
		columns.add("hits.product.productVariant");
		columns.add("hits.product.productBrand");
		columns.add("hits.product.productRevenue");
		columns.add("hits.product.localProductRevenue");
		columns.add("hits.product.productPrice");
		columns.add("hits.product.localProductPrice");
		columns.add("hits.product.productQuantity");
		columns.add("hits.product.productRefundAmount");
		columns.add("hits.product.localProductRefundAmount");
		columns.add("hits.product.isImpression");
		columns.add("hits.product.isClick");
		columns.add("hits.product.customDimensions.index");
		columns.add("hits.product.customDimensions.value");
		columns.add("hits.product.customMetrics.index");
		columns.add("hits.product.customMetrics.value");	
		columns.add("hits.product.productListName");
		columns.add("hits.product.productListPosition");
		
		columns.add("hits.promotion.promoId");
		columns.add("hits.promotion.promoName");
		columns.add("hits.promotion.promoCreative");
		columns.add("hits.promotion.promoPosition");
		columns.add("hits.promotionActionInfo.promoIsView");
		columns.add("hits.promotionActionInfo.promoIsClick");
			
		columns.add("hits.refund.refundAmount");
		columns.add("hits.refund.localRefundAmount");
		columns.add("hits.eCommerceAction.action_type");
		columns.add("hits.eCommerceAction.step");
		columns.add("hits.eCommerceAction.option");
		
		columns.add("hits.customVariables.index");
		columns.add("hits.customVariables.customVarName");
		columns.add("hits.customVariables.customVarValue");
		columns.add("hits.customDimensions.index");
		columns.add("hits.customDimensions.value");
		columns.add("hits.customMetrics.index");
		columns.add("hits.customMetrics.value");
		
		columns.add("hits.type");
		columns.add("hits.social.socialInteractionNetwork");
		columns.add("hits.social.socialInteractionAction");
		columns.add("hits.social.socialInteractions");
		columns.add("hits.social.socialInteractionTarget");
		columns.add("hits.social.socialNetwork");
		columns.add("hits.social.uniqueSocialInteractions");
		columns.add("hits.social.hasSocialSourceReferral");
		columns.add("hits.social.socialInteractionNetworkAction");
		
		columns.add("hits.contentGroup.contentGroup1");
		columns.add("hits.contentGroup.contentGroup2");
		columns.add("hits.contentGroup.contentGroup3");
		columns.add("hits.contentGroup.contentGroup4");
		columns.add("hits.contentGroup.contentGroup5");
		columns.add("hits.contentGroup.previousContentGroup1");
		columns.add("hits.contentGroup.previousContentGroup2");
		columns.add("hits.contentGroup.previousContentGroup3");
		columns.add("hits.contentGroup.previousContentGroup4");
		columns.add("hits.contentGroup.previousContentGroup5");
		columns.add("hits.contentGroup.contentGroupUniqueViews1");
		columns.add("hits.contentGroup.contentGroupUniqueViews2");
		columns.add("hits.contentGroup.contentGroupUniqueViews3");
		columns.add("hits.contentGroup.contentGroupUniqueViews4");
		columns.add("hits.contentGroup.contentGroupUniqueViews5");
		
		columns.add("hits.dataSource");
		columns.add("userId");
		columns.add("channelGrouping");
		columns.add("socialEngagementType");
						
		
		//Not required
		/*
		columns.add("visitorId");
		columns.add("totals.transactionRevenue");
		
		columns.add("device.browserVersion");
		columns.add("device.browserSize");
		columns.add("device.operatingSystemVersion");
		columns.add("device.mobileInputSelector");
		columns.add("device.mobileDeviceInfo");
		columns.add("device.flashVersion");
		columns.add("device.javaEnabled");
		columns.add("device.screenColors");
		columns.add("device.screenResolution");
		columns.add("hits.isSecure");
		columns.add("hits.isInteraction");
		columns.add("hits.page.pagePathLevel1");
		columns.add("hits.page.pagePathLevel2");
		columns.add("hits.page.pagePathLevel3");
		columns.add("hits.page.pagePathLevel4");
		columns.add("hits.item.productSku");
		/*
		columns.add("hits.exceptionInfo.description");
		columns.add("hits.exceptionInfo.isFatal");
		columns.add("hits.exceptionInfo.exceptions");
		columns.add("hits.exceptionInfo.fatalExceptions");
		*/
		/*
		columns.add("hits.experiment.experimentId");
		columns.add("hits.experiment.experimentVariant");
		*/
		/*
		columns.add("hits.publisher.dfpClicks");
		columns.add("hits.publisher.dfpImpressions");
		columns.add("hits.publisher.dfpMatchedQueries");
		columns.add("hits.publisher.dfpMeasurableImpressions");
		columns.add("hits.publisher.dfpQueries");
		columns.add("hits.publisher.dfpRevenueCpm");
		columns.add("hits.publisher.dfpRevenueCpc");
		columns.add("hits.publisher.dfpViewableImpressions");
		columns.add("hits.publisher.dfpPagesViewed");
		columns.add("hits.publisher.adsenseBackfillDfpClicks");
		columns.add("hits.publisher.adsenseBackfillDfpImpressions");
		columns.add("hits.publisher.adsenseBackfillDfpMatchedQueries");
		columns.add("hits.publisher.adsenseBackfillDfpMeasurableImpressions");
		columns.add("hits.publisher.adsenseBackfillDfpQueries");
		columns.add("hits.publisher.adsenseBackfillDfpRevenueCpm");
		columns.add("hits.publisher.adsenseBackfillDfpRevenueCpc");
		columns.add("hits.publisher.adsenseBackfillDfpViewableImpressions");
		columns.add("hits.publisher.adsenseBackfillDfpPagesViewed");
		columns.add("hits.publisher.adxBackfillDfpClicks");
		columns.add("hits.publisher.adxBackfillDfpImpressions");
		columns.add("hits.publisher.adxBackfillDfpMatchedQueries");
		columns.add("hits.publisher.adxBackfillDfpMeasurableImpressions");
		columns.add("hits.publisher.adxBackfillDfpQueries");
		columns.add("hits.publisher.adxBackfillDfpRevenueCpm");
		columns.add("hits.publisher.adxBackfillDfpRevenueCpc");
		columns.add("hits.publisher.adxBackfillDfpViewableImpressions");
		columns.add("hits.publisher.adxBackfillDfpPagesViewed");
		columns.add("hits.publisher.adxClicks");
		columns.add("hits.publisher.adxImpressions");
		columns.add("hits.publisher.adxMatchedQueries");
		columns.add("hits.publisher.adxMeasurableImpressions");
		columns.add("hits.publisher.adxQueries");
		columns.add("hits.publisher.adxRevenue");
		columns.add("hits.publisher.adxViewableImpressions");
		columns.add("hits.publisher.adxPagesViewed");
		columns.add("hits.publisher.adsViewed");
		columns.add("hits.publisher.adsUnitsViewed");
		columns.add("hits.publisher.adsUnitsMatched");
		columns.add("hits.publisher.viewableAdsViewed");
		columns.add("hits.publisher.measurableAdsViewed");
		columns.add("hits.publisher.adsPagesViewed");
		columns.add("hits.publisher.adsClicked");
		columns.add("hits.publisher.adsRevenue");
		columns.add("hits.publisher.dfpAdGroup");
		columns.add("hits.publisher.dfpAdUnits");
		columns.add("hits.publisher.dfpNetworkId");
		*/
		/*
		columns.add("hits.latencyTracking.pageLoadSample");
		columns.add("hits.latencyTracking.pageLoadTime");
		columns.add("hits.latencyTracking.pageDownloadTime");
		columns.add("hits.latencyTracking.redirectionTime");
		columns.add("hits.latencyTracking.speedMetricsSample");
		columns.add("hits.latencyTracking.domainLookupTime");
		columns.add("hits.latencyTracking.serverConnectionTime");
		columns.add("hits.latencyTracking.serverResponseTime");
		columns.add("hits.latencyTracking.domLatencyMetricsSample");
		columns.add("hits.latencyTracking.domInteractiveTime");
		columns.add("hits.latencyTracking.domContentLoadedTime");
		columns.add("hits.latencyTracking.userTimingValue");
		columns.add("hits.latencyTracking.userTimingSample");
		columns.add("hits.latencyTracking.userTimingVariable");
		columns.add("hits.latencyTracking.userTimingCategory");
		columns.add("hits.latencyTracking.userTimingLabel");
		*/
		/*
		columns.add("hits.sourcePropertyInfo.sourcePropertyDisplayName");
		columns.add("hits.sourcePropertyInfo.sourcePropertyTrackingId");
		*/
	}

	public List<String> getColumns() {
		return columns;
	}
	
	public void createFolder(String folder) {
		this.filepath = PATH_PREFIX + File.separatorChar + folder;
		Path path = Paths.get(filepath);
		try {
			if(!Files.exists(path))
				Files.createDirectories(path);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	public void createFile(String filename, boolean withHeader, String delimiter, boolean withQuote) {
		this.delimiter = delimiter == null ? this.delimiter : delimiter;
		this.withQuote = withQuote;
		filepath = this.filepath == null ? PATH_PREFIX : filepath;
		
		try {
			pw = new PrintWriter(new File(filepath + File.separatorChar + filename));
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		if(withHeader)
			writeHeader();
	}
	
	public void appendFile(String filename, String delimiter, boolean withQuote) {
		this.delimiter = delimiter == null ? this.delimiter : delimiter;
		this.withQuote = withQuote;
		filepath = this.filepath == null ? PATH_PREFIX : filepath;
		
		try {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(filepath + File.separatorChar + filename, true)));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void closeFile() {
		pw.flush();
		pw.close();
	}
	
	public void moveCompletedFile(String filename, String destination) throws IOException {
		String source = this.filepath == null ? PATH_PREFIX : filepath;
		String target = PATH_PREFIX + File.separatorChar + destination;
		
		Path path = Paths.get(target);
		try {
			if(!Files.exists(path))
				Files.createDirectories(path);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		Files.move(FileSystems.getDefault().getPath(source, filename)
				, FileSystems.getDefault().getPath(target, filename), StandardCopyOption.ATOMIC_MOVE);
	}
	
	private void writeHeader() {
		
		boolean isFirstColumn = true;
		for(String column : columns) {
			if(isFirstColumn) {
				writeValue(column);
				isFirstColumn = false;
			} else {
				writeField(column, false);
			}
		}
	}
	
	private void writeValue(Object value) {
		String v = value == null ? "" : value.toString()
											.replace("\\", "\\\\")						//replace '\' with '\\' 
											.replace("\"", "\\\"")						//replace '"' with '\"'
											.replace(delimiter, "\\" + delimiter)		//escape delimiter with '\'
											.replace("\r", "")							//remove carriage return
											.replace("\n", "\\\n");						//escape newline
		
		//Add a space at the end to avoid backslash clashing with quote
		//if(v.endsWith("\\"))
		//	v = v + " ";
		if(withQuote)
			pw.printf("\"%s\"", v);	
		else
			pw.printf("%s%s", v);
	}
	
	public void writeField(Object value, boolean firstColumn) {
		if(firstColumn)
			pw.println();
		else
			pw.print(delimiter);
		
		writeValue(value);		
	}
	
}
