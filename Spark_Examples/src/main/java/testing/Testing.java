package testing;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class Testing {
	public static void main(String[] args) {
		PrintWriter out;
		try {
			out = new PrintWriter("Test.txt");
			for(int i = 0; i <= 100000; i++)
			{
			    out.println(i);
			    
			}
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}
}
