package job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperThree extends Mapper <Object, Text, Text, Text>{
	
	
	private final int NAME = 0;
	private final int TICKER = 1;
	private final int VARIAZIONE2016 = 2;
	private final int VARIAZIONE2017 = 3;
	private final int VARIAZIONE2018 = 4;
	
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String input = value.toString();
		
		String[] campi = input.split(",");
		
		
		context.write(new Text(campi[VARIAZIONE2016]+","+campi[VARIAZIONE2017]+","+campi[VARIAZIONE2018]), new Text(campi[NAME]+","+campi[TICKER]));
		
	} 

}
