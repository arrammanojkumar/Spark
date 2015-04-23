package mongoDBPractice;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * It uses Poi ooxml
 * @author marram
 *
 */
public class CreateExcelFile {
	public static void main(String[] args) {
		try {
			@SuppressWarnings("resource")
			Workbook wb = new XSSFWorkbook();
			FileOutputStream fileOut = new FileOutputStream("workbook.xlsx");
			wb.write(fileOut);
			System.out.println("DOne");
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
