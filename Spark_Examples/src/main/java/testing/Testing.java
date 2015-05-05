package testing;

import java.net.UnknownHostException;
import java.util.ArrayList;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.MongoClient;

public class Testing {

	public static DB db;

	// public static ArrayList<String> activity_OutcomeList = new
	// ArrayList<String>();

	public static void main(String[] args) {
		
		ArrayList<String> activity_OutcomeList = new ArrayList<String>();

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

				DBObject oo = (DBObject) cursor.next();
				DBObject object = (DBObject) oo.get("startupCompanyInfo");
				DBObject ob = (DBObject) object
						.get("startupIndustryMappingInfos");
				JSONArray array = new JSONArray(ob.toString());
				String name_of_company = oo.get("name").toString();

				for (int i = 0; i < array.length(); i++) {

					JSONObject startupUsecaseInfoObject = new JSONObject(
							array.getString(i));
					if (startupUsecaseInfoObject.has("startupUseCaseInfo")) {
						JSONArray outcomes_ActivitiesArray = (JSONArray) (startupUsecaseInfoObject
								.get("startupUseCaseInfo"));

						if (outcomes_ActivitiesArray != null) {

							JSONObject sectorInfoRef = (JSONObject) startupUsecaseInfoObject
									.get("sectorInfo");

							String industryName = "";
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

									String outComeName = "", activityName = "";
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

									String record = name_of_company + " - "
											+ outComeName + " - " + " - "
											+ activityName + " - "
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
}
