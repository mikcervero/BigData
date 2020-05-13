package job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PricesMapper extends Mapper<Object, Text, Text, Text> {

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

			String[] data = campi[DATE].split("-");

			int anno = Integer.parseInt(data[0]);

			if (anno >= 2008 && anno <= 2018) {

				//prices permette di distinguere il dataset di provenienza nel join
				context.write(new Text(campi[SYMBOL]), new Text("prices" + "," + campi[PREZZOCHIUSURA] + "," + campi[VOLUME] + "," + campi[DATE]));
			}

			
		}
		
		else {
			context.getCounter(COUNTERS2.INVALID_RECORD_COUNT_JOB2).increment(1L);
		}

	}
}