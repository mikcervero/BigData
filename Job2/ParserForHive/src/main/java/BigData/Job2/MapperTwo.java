package BigData.Job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapperTwo extends Mapper<Object, Text, Text, Text> {

	public enum COUNTERS2 {
		INVALID_RECORD_COUNT_JOB2
	}

	private final int SYMBOL = 0;
	private final int PREZZOCHIUSURA = 2;
	private final int VOLUME = 6;
	private final int DATE = 7;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String input = value.toString();
		String[] campi = input.split(",");

		if (campi.length == 8) {

			context.write(new Text(campi[SYMBOL]),
					new Text("prices" + ","+ campi[PREZZOCHIUSURA] + "," + campi[VOLUME] + "," + campi[DATE]));
		}

		else {
			context.getCounter(COUNTERS2.INVALID_RECORD_COUNT_JOB2).increment(1L);
		}

	}

}
