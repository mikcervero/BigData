package job3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerThree extends Reducer<Text, Text, Text, Text> {	

	public void reduce(Text variazioni, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		

		
		List<String> aziende= new ArrayList<String>();
		List<String> tickers= new ArrayList<String>();

		for (Text value : values) {
			
			String azienda= value.toString().split(",")[0];
			String ticker= value.toString().split(",")[1];
			
			aziende.add(azienda);
			tickers.add(ticker);
			}
		
		String[]variazione=variazioni.toString().split(",");
		
	 
		   
		if(aziende.size()>1)
		
			context.write(new Text(aziende.toString()+"   "+ tickers.toString()), new Text("     2016:"+variazione[0]+"%"+"    2017:"+variazione[1]+"%"+"    2018:"+variazione[2]+"%"));
	
	
	  }

}
