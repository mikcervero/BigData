package job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperTwo extends Mapper <Object, Text, Text, Text>{
	
	private final int SECTOR = 1;
	private final int DATE = 5;
	private final int VOLUME = 3;
	private final int TICKER = 0;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String input = value.toString();
		
		String[] campi = input.split(",");
		
		String anno = campi[DATE].split("-")[0];
		
		context.write(new Text(campi[SECTOR]+","+anno), new Text(campi[TICKER]+","+campi[VOLUME]+","+campi[DATE]));
		
	}

}