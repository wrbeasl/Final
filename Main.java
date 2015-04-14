import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;


public class Main{
	
	static HashMap<String, String> matchMap = new HashMap<String, String>();
	
	public static Map<String, Vector<String> > getHP() throws IOException{
		Map<String, Vector<String> >temp = new HashMap<String, Vector<String> >();
		
		PrintWriter pw = new PrintWriter("helpfulness.txt", "UTF-8");
		InputStream is = new FileInputStream("movies.txt");
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		
		String line = null;
		String PID = null;
		String time = null;
		String help = null;
		while((line = br.readLine()) != null){
			String[] temp1 = line.split(" ");
			if(temp1[0].contains("help")) help = temp1[1];
			if(temp1[0].contains("time")) time = temp1[1];
			if(temp1[0].contains("Id")) PID = temp1[1];
			if(PID!=null&&time!=null&&help!=null&&matchMap.containsKey(PID)){
				Vector<String> t = new Vector<String>();
				t.addElement(time);
				t.addElement(help);
				temp.put(PID,t);
				PID=null;
				time=null;
			}
		}
		Set<String> ss = temp.keySet();
		for(int i = 0; i < temp.size(); ++i){
			Vector<String> t = temp.get(ss.toArray()[i]);
			System.out.println(ss.toArray()[i] + "::" + t.elementAt(0) + "::" + t.elementAt(1));
			pw.println(ss.toArray()[i] + "::" + t.elementAt(0) + "::" + t.elementAt(1));
			
		}
		
		pw.close();
		is.close();
		isr.close();
		br.close();
		
		return temp;
	}
	
	public static void genDates() throws IOException{
		Parse parse = new Parse();
		PrintWriter pw = new PrintWriter("release_dates.txt", "UTF-8");
		InputStream is = new FileInputStream("movies.txt");
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		
		String line = null;
		String PID;
		while((line = br.readLine()) != null){
			String[] temp1 = line.split(" ");
			if(temp1[0].contains("Id")){
				PID = temp1[1];
				String date = parse.getDate(PID);
				pw.write(PID + " " + date);
			}
		}
		
		pw.close();
		is.close();
		isr.close();
		br.close();
		
	}
	
	public static void setup() throws IOException{
		InputStream is = new FileInputStream("release_dates.txt");
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		
		String line = null;
		while((line = br.readLine())!=null){
			String[] split = line.split(" ");
			matchMap.put(split[0], split[1]);
		}
		
		is.close();
		isr.close();
		br.close();
	}
	
	public static void main(String[] args) throws IOException{
		genDates();
		setup();
		getHP();
	}

}