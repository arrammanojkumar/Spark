package mongoDBPractice;

import java.net.UnknownHostException;
import java.util.ArrayList;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoClient;

public class MongoDBJDBC {

	public static String dbName = "bridge";
	public static String collection = "innovator";
	public static String server = "localhost";
	public static int port = 27017;

	public static DB db;
	public static ArrayList<ArrayList<Object>> startupList;
	public static ArrayList<String> enterpirseList;
	public static ArrayList<String> activity_OutcomeList;

	public static String enterpriseHeader[] = { "_id", "name", "sector",
			"foundedIn", "status" };

	public static String statupHeader[] = { "_id", "recordtype", "video",
			"lastmodified", "sequenceId", "team", "year", "batch",
			"ds_company_data_id", "awards", "batchId", "name", "_class",
			"innovatorsId", "city", "company_news_page", "industry", "news",
			"funding", "products", "linkedin_profile", "status",
			"company_pressrelease_page", "company_size", "customer_type",
			"logo", "last_field_changed", "website", "customers", "country",
			"company_name", "industried_served", "website_conformed",
			"company_blog", "case_studies", "compnay_id", "loadingDate",
			"state" };

	public static String activity_OutcomeHeader[] = { "Company Name",
			"Outcome Name", "Activity Name", "Industry Name" };

	public static void main(String args[]) throws JSONException,
			UnknownHostException {

		db = new MongoClient("localhost", 27017).getDB("dbridge");

		// StartupSheet
		// createStartupSheet();
		//
		// dbName = "dbridge";
		// collection = "companyInfo";
		// // Enterprise Sheet
		// createEnterpriseSheet();
		// // Outcomes Sheet
		// createOutcomesSheet();
		//
		// //Activities Shet
		// createActivitiesSheet();
		//
		// // Writing into Excel File
		// CreateExcelFile.writeIntoExcel(startupList, enterpirseList,
		// outcomesList, activitiesList ,statupHeader, enterpriseHeader,
		// outcomesHeader, activityHeader);

		CreateExcelFile.writeIntoActivityAndOutcomeSheet(
				createActivity_OutcomeSheet(), activity_OutcomeHeader,
				CreateExcelFile.activity_outcomes);
	}

	public static ArrayList<String> createActivity_OutcomeSheet() {
		activity_OutcomeList = new ArrayList<String>();

		try {

			db = new MongoClient("localhost", 27017).getDB("dbridge");

			BasicDBObject whereQuery = new BasicDBObject();
			BasicDBObject subWhereQuery = new BasicDBObject();
			subWhereQuery.put("$exists", true);
			whereQuery.put("startupCompanyInfo", subWhereQuery);

			DBCursor cursor = db.getCollection("companyInfo").find(whereQuery);

			/**
			 * startupCompanyInfo ---> startupIndustryMappingInfos --> In Array
			 * --> startupUseCaseInfo --> Array --> Outcomes And Activities
			 * Master
			 * 
			 * startupCompanyInfo ---> startupIndustryMappingInfos --> In Array
			 * --> Sector Info
			 */
			while (cursor.hasNext()) {

				DBObject document = (DBObject) cursor.next();
				DBObject startupCompanyInfoDocument = (DBObject) document
						.get("startupCompanyInfo");
				DBObject startupIndustryMappingInfoDocument = (DBObject) startupCompanyInfoDocument
						.get("startupIndustryMappingInfos");
				JSONArray array = new JSONArray(
						startupIndustryMappingInfoDocument.toString());
				String name_of_company = document.get("name").toString();

				for (int i = 0; i < array.length(); i++) {

					JSONObject startupUsecaseInfoObject = new JSONObject(
							array.getString(i));
					if (startupUsecaseInfoObject.has("startupUseCaseInfo")) {
						JSONArray outcomes_ActivitiesArray = (JSONArray) (startupUsecaseInfoObject
								.get("startupUseCaseInfo"));

						if (outcomes_ActivitiesArray != null) {

							JSONObject sectorInfoRef = (JSONObject) startupUsecaseInfoObject
									.get("sectorInfo");

							String industryName = "N/A";
							// Industry Name
							if (sectorInfoRef != null) {

								industryName = getIndustryUsingSector(
										sectorInfoRef.get("$ref").toString(),
										sectorInfoRef.get("$id"));
							}

							for (int j = 0; j < outcomes_ActivitiesArray
									.length(); j++) {
								for (int k = 0; k < outcomes_ActivitiesArray
										.length(); k++) {
									JSONObject Json = (JSONObject) outcomes_ActivitiesArray
											.get(k);

									String outComeName = "N/A", activityName = "N/A";

									System.out.println("outcome master : "+Json.has("outcomesMaster"));
									System.out.println("activitiesMaster : "+Json.has("activitiesMaster"));
									
									if (Json.has("outcomesMaster")) {
										JSONObject outcomeMasterJSON = (JSONObject) Json
												.get("outcomesMaster");
										outComeName = getOutcome_Activity(
												outcomeMasterJSON.get("$ref")
														.toString(),
												outcomeMasterJSON.get("$id"));
									} else {
										outComeName = " N/A";
									}
									if (Json.has("activitiesMaster")) {
										JSONObject activityMasterJSON = (JSONObject) Json
												.get("activitiesMaster");
										activityName = getOutcome_Activity(
												activityMasterJSON.get("$ref")
														.toString(),
												activityMasterJSON.get("$id"));
									} else {
										activityName = "N/A";
									}

									String record = name_of_company + "$$"
											+ outComeName + "$$" + "$$"
											+ activityName + "$$"
											+ industryName;

									activity_OutcomeList.add(record);
								}
							}
						}
					}
				}
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return activity_OutcomeList;
	}

	public static String getOutcome_Activity(String refCollection, Object refId) {
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("_id", refId);

		DBCursor cursor = db.getCollection(refCollection).find(whereQuery);

		while (cursor.hasNext())
			return cursor.next().get("name").toString();
		return null;
	}

	public static String getIndustryUsingSector(String refCollection,
			Object refId) {
		String industryNames = "";

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("_id", refId);

		DBCursor cursor1 = db.getCollection(refCollection).find(whereQuery);

		while (cursor1.hasNext()) {
			DBObject sectorDocument = cursor1.next();

			if (sectorDocument.get("industryMaster") != null) {
				DBRef indRef = (DBRef) sectorDocument.get("industryMaster");
				String indCollection = indRef.getRef();
				Object indRefid = indRef.getId();

				BasicDBObject whereQuery1 = new BasicDBObject();
				whereQuery1.put("_id", indRefid);

				DBCursor cursor2 = db.getCollection(indCollection).find(
						whereQuery1);

				while (cursor2.hasNext()) {
					industryNames = (cursor2.next().get("industryName")
							.toString());
				}
				cursor2.close();
			}
		}
		cursor1.close();
		return industryNames;
	}

	public static void createEnterpriseSheet() {

		DBCursor cursor = getDBCursor(dbName, server, port, collection);

		enterpirseList = new ArrayList<String>();

		while (cursor.hasNext()) {
			DBObject document = cursor.next();

			if (document.get("enterpriseCompanyInfo") != null) {
				for (String head : enterpriseHeader) {
					if (document.get(head) != null)
						enterpirseList.add(document.get(head).toString());
					else
						enterpirseList.add("N/A");
				}
				enterpirseList.add("--END--");
			}
		}
	}

	public static void createStartupSheet() {
		// Get the connection to MongoDB database

		DBCursor cursor = getDBCursor(dbName, server, port, collection);
		startupList = new ArrayList<ArrayList<Object>>();

		while (cursor.hasNext()) {

			DBObject document = cursor.next();
			ArrayList<Object> documentList = new ArrayList<Object>();

			for (String head : statupHeader) {
				documentList.add(document.get(head));
			}
			startupList.add(documentList);
		}
	}

	/**
	 * Connect to Mongo DB database
	 * 
	 * @param dbName
	 * @param server
	 * @param port
	 * @param collectionName
	 * @return DB Cursor
	 */
	public static DBCursor getDBCursor(String dbName, String server, int port,
			String collectionName) {
		return db.getCollection(collectionName).find();
	}

	/**
	 * 
	 * @param dbName
	 * @param server
	 * @param port
	 * @return DB handle
	 */
	public static DB getDB(String dbName, String server, int port) {
		return db;
	}

	public static DBCollection getDBCollection(String dbName, String server,
			String collection, int port) {
		return db.getCollection(collection);
	}
}