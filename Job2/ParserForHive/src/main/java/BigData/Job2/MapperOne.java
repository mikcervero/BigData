package BigData.Job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperOne extends Mapper<Object, Text, Text, Text> {

	public enum COUNTERS1 {
		INVALID_RECORD_COUNT_JOB1
	}

	private final int SYMBOL = 0;
	private final int NAME = 2;
	private final int SECTOR = 3;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Parser parser = new Parser();
		String[] campi = parser.processString(value.toString()).split(",");

		if (campi.length == 5 && campi != null) {

			context.write(new Text(campi[SYMBOL]), new Text("stocks" +","+ campi[SECTOR]+","+ campi[NAME]));
		}

		else {
			context.getCounter(COUNTERS1.INVALID_RECORD_COUNT_JOB1).increment(1L);
		}

	}

}
