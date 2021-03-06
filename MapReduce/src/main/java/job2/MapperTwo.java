package job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTwo extends Mapper <Object, Text, Text, Text>{
	
	private final int TICKER = 0;
	private final int SECTOR = 1;
	private final int VOLUME = 2;
	private final int CLOSE = 3;
	private final int DATE = 4;
	
	
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String input = value.toString();
		
		String[] campi = input.split(",");
		
		
		// date nel formato year-month-day
		String anno = campi[DATE].split("-")[0];
		
		// chiave: settore e anno 
		context.write(new Text(campi[SECTOR]+"  "+anno), new Text(campi[TICKER]+","+campi[VOLUME]+","+campi[CLOSE]+","+campi[DATE]));
		
	}

}
