package parser;



public class Parser 
{
	private final int TICKER = 0;
	private final int NAME = 2;
	
	
	public Parser() {};
	


  public String processString (String text)	{
	  
	    String[] fields= text.split(",");
		String ticker= fields[TICKER];
		String sector= fields[fields.length-2];
		String industry=fields[fields.length-1];
		String result = null;
		
		
		
        if(industry.indexOf('"')!=-1) {
			
			sector= fields[fields.length-3];
		}
		
		
		
		
		if(sector.equals("N/A")) {
			
			return result; 
		}
		
		
		
				
		if(fields[NAME].indexOf('"')!=-1) {
			fields[NAME]=fields[2].replace('"', ' ');
			fields[3]=fields[3].replace('"', ' ');
			fields[NAME]=fields[2]+fields[3];
	        
		}
		
		
				
			 return result= ticker + "," + fields[NAME] + "," + sector ;
			 	
  }
  

}
    



