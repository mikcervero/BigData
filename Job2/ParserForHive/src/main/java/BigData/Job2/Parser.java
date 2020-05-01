package BigData.Job2;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Parser extends UDF
{
	private final int TICKER = 0;
	private final int NAME = 2;
	
	
	public static void main(String[] args) throws Exception {
		
		String prova =" GMED,NYSE,\"GLOBUS MEDICAL, INC.\",N/A,N/A";
		
		
		String result= rowParser(prova);
		
		
		
		System.out.print(result);
		
		}
	
   public static String rowParser(String text) {
    	
    	if(text == null) return null;
    	
    	String[] fields= text.toString().split(",");
    	
    	String ticker= fields[0];
    	String sector= fields[fields.length-2];
    	
    	if(sector.equals("N/A")) {
    		
    		return null;
    	}
    	
    	if(ticker.indexOf('^')!=-1 || ticker.indexOf('.')!=-1 || ticker.indexOf('~')!=-1) {
    		
    		return null;
    	}
    	
    	
    	if(fields[2].indexOf('"')!=-1) {
			fields[2]=fields[2].replace('"', ' ');
			fields[3]=fields[3].replace('"', ' ');
			fields[2]=fields[2]+fields[3];
			
			String result = ticker + "," +  fields[2] + "," + sector;
			
			return result;
		}
		
		
		
		String result = ticker + "," +  fields[2] + "," + sector;
		
    	
    	return result;	
    		
    	}
    	
		
	
	
   /* public static Text rowParser(Text text) {
    	
    	if(text == null) return null;
    	
    	String[] fields= text.toString().split(",");
    	
    	String ticker= fields[0];
    	String sector= fields[fields.length-2];
    	
    	if(sector.equals("N/A")) {
    		
    		return null;
    	}
    	
    	if(ticker.indexOf('^')!=-1 || ticker.indexOf('.')!=-1 || ticker.indexOf('~')!=-1) {
    		
    		return null;
    	}
    	
    	
    	if(fields[2].indexOf('"')!=-1) {
			fields[2]=fields[2].replace('"', ' ');
			fields[3]=fields[3].replace('"', ' ');
			fields[2]=fields[2]+fields[3];
			
			Text result = new Text(ticker + "," +  fields[2] + "," + sector);
			
			return result;
		}
		
		
		
		Text result = new Text(ticker + "," +  fields[2] + "," + sector);
		
    	
    	return result;	
    		
    	}*/
    	
    	
      	
    	
    }
    



