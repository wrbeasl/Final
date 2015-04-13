import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Parse {
	    public Parse(){}
	    public String getDate(String ASIN) {
	    	ASIN = ASIN.trim();
	    	int num_of_fails = 0;
	        try {
	            doc = Jsoup.connect("http://www.amazon.com/gp/product/" + ASIN+"/").timeout(30000).userAgent("Mozilla/17.0").get();           	
	        }
	        catch (java.io.IOException e) {
	        	num_of_fails++;
	        }
	        
	    	try {
				doc = Jsoup.connect("http://www.amazon.com/exec/obidos/ASIN/" + ASIN).timeout(30000).userAgent("Mozilla/17.0").get();
			} catch (IOException e1) {
				num_of_fails++;
			}
	    	
	    	try{
	            doc = Jsoup.connect("http://www.amazon.com/dp/" + ASIN+"/").timeout(30000).userAgent("Mozilla/17.0").get();           	
	    	} catch(IOException e2){
	        	num_of_fails++;
	    	}
	    	
	    	if(num_of_fails >= 3){
	    		return null;
	    	}


	        Elements lines = doc.select("li");
	        
	        String rDate = null;

	        for(Element e: lines) {

	            if(e.toString().contains("Release Date")) {
	                rDate = e.toString();
	                rDate = rDate.substring(rDate.lastIndexOf("b>") + 2, rDate.lastIndexOf("<"));
	            }

	        }


	    return rDate;
	    }

	    private static Document doc;
}	