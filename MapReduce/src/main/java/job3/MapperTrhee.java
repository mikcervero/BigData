package job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperTrhee extends Mapper <Object, Text, Text, Text>{
	
	private final int TICKER = 0;
	private final int NAME = 1;
	private final int CLOSE = 2;
	private final int DATE = 3;
	
	
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String input = value.toString();
		
		String[] campi = input.split(",");
		
		String anno = campi[DATE].split("-")[0];
		
		context.write(new Text(campi[NAME]+","+ campi[TICKER]), new Text(campi[CLOSE]+","+campi[DATE]+ ","+ anno));
		
	} 

}
