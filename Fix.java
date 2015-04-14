import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;


public class Fix {
	public void main(String[] args) throws IOException{
		//Parse parse = new Parse();
		PrintWriter pw = new PrintWriter("release_dates_.txt", "UTF-8");
		InputStream is = new FileInputStream("release_dates.txt");
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		
		String line = null;
		String PID;
		while((line = br.readLine()) != null){
			String out = line.substring(line.indexOf(",")+5, line.length());
			String out1 = line.substring(0, line.indexOf(",")+5);
			
			System.out.println(out+" "+out1);
			
		}
		
		pw.close();
		is.close();
		isr.close();
		br.close();
	}
}
