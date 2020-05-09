package parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser 
{
	private final int TICKER = 0;
	private final int NAME = 2;
	
	
	public Parser() {};
	

 

  public String processString (String text)	{
	  
	    String[] fields= text.split(",");
		String ticker= fields[0];
		String sector= fields[fields.length-2];
		String industry=fields[fields.length-1];
		String result = null;
		
		
		
        if(industry.indexOf('"')!=-1) {
			
			sector= fields[fields.length-3];
		}
		
		
		
		
		if(sector.equals("N/A")) {
			
			return result; 
		}
		
		
		
				
		if(fields[2].indexOf('"')!=-1) {
			fields[2]=fields[2].replace('"', ' ');
			fields[3]=fields[3].replace('"', ' ');
			fields[2]=fields[2]+fields[3];
	        
		}
		
		
				
			 return result= ticker + "," + fields[2] + "," + sector ;
			 	
  }
  

}
    



