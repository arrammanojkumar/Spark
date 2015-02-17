package usingPartitioner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class generateInputFile {

	private static final String CHAR_LIST = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	private static final int lineLength = 10;

	public static void main(String[] args) {
		try {
			File file = new File("usingPartitioner.txt");
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter writer = new BufferedWriter(fw);
			
			for(int i = 0 ; i < 5000; i++)
			{
				String line = (i+1)+"\t"+generateLine()+"\n";
				System.out.println(line);
				writer.write(line);
			}
		
			System.out.println("Done");
			
			writer.close();
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static String generateLine()
	{
		String line = "";
		for (int i = 0 ; i < lineLength; i++)
		{
			int randomNumber = getRandomNumber();
			line += CHAR_LIST.charAt(randomNumber); 
		}
		return line;
	}
	
	public static int getRandomNumber()
	{
		Random no = new Random();
		int random = no.nextInt(CHAR_LIST.length());
		
		//IF we dont do this we may get AIOB Exception
		if(random - 1 == -1)
			return random;
		else 
			return random-1;
	}
}
