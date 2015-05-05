package mongoDBPractice;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.types.ObjectId;

import com.mongodb.util.JSON;

/**
 * It uses Poi ooxml
 * 
 * @author marram
 *
 */
public class CreateExcelFile {

	private static Workbook wb = new XSSFWorkbook();

	private static Sheet sheetStartupFeatures = wb
			.createSheet("StartupFeatures");
	private static Sheet sheetEnterprises = wb.createSheet("Enterprises");

	public static Sheet activity_outcomes = wb.createSheet("Activity_Outcomes");

	public static void writeIntoExcel(ArrayList<ArrayList<Object>> startupList,
			ArrayList<String> enterprisesList, ArrayList<String> outcomesList,
			ArrayList<String> activitiesList, String[] startupHeader,
			String[] enterpriseHeader, String[] outcomesHeader,
			String[] activityHeader) {

		// writeIntoStartupSheet(startupList, startupHeader);

		// writeIntoSheet(enterprisesList, enterpriseHeader, sheetEnterprises);
		// writeIntoSheet(outcomesList, outcomesHeader, outcomes);
		// writeIntoSheet(activitiesList, activityHeader, activities);

		writeIntoFile();

	}

	public static void writeIntoActivityAndOutcomeSheet(ArrayList<String> list,
			String[] header, Sheet sheetName) {
		int rowCount = 0;

		// Header
		Row row = sheetName.createRow(rowCount);

		for (int i = 0; i < header.length; i++) {
			row.createCell(i).setCellValue(header[i]);
		}
		rowCount++;

		for (String record : list) {

			row = sheetName.createRow(rowCount);

			StringTokenizer tokens = new StringTokenizer(record, "$$");
			int i = 0;
			while (tokens.hasMoreTokens()) {
				row.createCell(i).setCellValue(tokens.nextToken().toString());
				i++;
			}
			rowCount++;
		}

		writeIntoFile();
	}

	public static void writeIntoSheet(ArrayList<String> list, String[] header,
			Sheet sheetName) {

		int rowCount = 0;
		int cellCount = 0;
		// Header
		Row row = sheetName.createRow(rowCount);

		for (int i = 0; i < header.length; i++) {
			row.createCell(i).setCellValue(header[i]);
		}
		rowCount++;

		// First row
		row = sheetName.createRow(rowCount);

		for (int i = 0; i < list.size(); i++) {
			if (list.get(i) == "--END--") {
				row = sheetName.createRow(rowCount);
				rowCount++;
				cellCount = 0;
			} else {
				row.createCell(cellCount).setCellValue(list.get(i).toString());
				cellCount++;
			}
		}
	}

	public static void writeIntoStartupSheet(
			ArrayList<ArrayList<Object>> collectionList, String[] startupHeader) {

		// Creating Header
		Row row = sheetStartupFeatures.createRow(0);

		for (int i = 0; i < startupHeader.length; i++) {
			row.createCell(i).setCellValue(startupHeader[i]);
		}

		try {
			for (int i = 0; i < collectionList.size(); i++) {

				ArrayList<Object> document = collectionList.get(i);
				row = sheetStartupFeatures.createRow(i + 1);

				for (int j = 0; j < document.size(); j++) {

					if (document.get(j) == null) {
						row.createCell(j).setCellValue("N/A");
						continue;
					}

					if (document.get(j) instanceof ObjectId) {
						row.createCell(j).setCellValue(
								JSON.serialize(document.get(j).toString()));
						continue;
					}

					row.createCell(j).setCellValue(document.get(j).toString());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void writeEnterpriseSheet(ArrayList<String> enterprisesList,
			String[] enterpriseHeader) {

		int rowCount = 0;
		int cellCount = 0;
		// Header
		Row row = sheetEnterprises.createRow(rowCount);

		for (int i = 0; i < enterpriseHeader.length; i++) {
			row.createCell(i).setCellValue(enterpriseHeader[i]);
		}
		rowCount++;

		// First row
		row = sheetEnterprises.createRow(rowCount);

		for (int i = 0; i < enterprisesList.size(); i++) {
			if (enterprisesList.get(i) == "--END--") {
				row = sheetEnterprises.createRow(rowCount);
				rowCount++;
				cellCount = 0;
			} else {
				row.createCell(cellCount).setCellValue(
						enterprisesList.get(i).toString());
				cellCount++;
			}
		}
	}

	public static void writeIntoFile() {
		FileOutputStream fileOut;
		try {
			fileOut = new FileOutputStream("BridgeDatasetsNew.xlsx");
			wb.write(fileOut);
			System.out.println("Done Writing");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
