package job2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerTwo extends Reducer<Text, Text, Text, Text> {

	private final int VOLUME = 1;

	public void reduce(Text sectoryear, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Long> tickervolume = new HashMap<String, Long>();

		long numberOfRecord = 0;
		long sumVolume = 0;
		double averageVolume = 0;

		for (Text value : values) {
			String[] input = value.toString().split(",");
			String ticker = input[0];
			long volume = Long.parseLong(input[VOLUME]);
			//numberOfRecord++;
		//	sumVolume += volume;
			
			if (tickervolume.containsKey(ticker)) {
				tickervolume.put(ticker, tickervolume.get(ticker)+volume);
			}
			else {
				tickervolume.put(ticker,volume);
			}

		}
		//float allVolumesSum = 0;
		for (long volume : tickervolume.values()) {
			sumVolume += volume;
		}
		double volumeAvg = sumVolume / (tickervolume.keySet().size());
		context.write(sectoryear, new Text(volumeAvg + ""));

	}

}
