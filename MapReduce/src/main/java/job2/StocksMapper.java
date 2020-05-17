package job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import parser.Parser;

public class StocksMapper extends Mapper<Object, Text, Text, Text> {

	public enum COUNTERS1 {
		INVALID_RECORD_COUNT_JOB1
	}

	private final int SYMBOL = 0;
	private final int SECTOR = 2;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Parser parser = new Parser();
		String[] campi = {};
		String row = parser.processString(value.toString());

		if (row != null) {

			campi = row.split(",");

		}

		if (campi.length == 3) {
			//stocks permette di distinguere il dataset di provenienza nel join
			context.write(new Text(campi[SYMBOL]), new Text("stocks" + "," + campi[SECTOR]));
		}

		else {
			context.getCounter(COUNTERS1.INVALID_RECORD_COUNT_JOB1).increment(1L);
		}

	}

}
