package job2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerTwo extends Reducer<Text, Text, Text, Text> {

	private final int VOLUME = 2;
	private final int DATE = 4;
	private final int CLOSE = 3;
	private final int TICKER = 0;

	public void reduce(Text sectoryear, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Long> tickervolume = new HashMap<String, Long>();
		Map<String, Integer> tickerdatemin = new HashMap<String, Integer>();
		Map<String, Integer> tickerdatemax = new HashMap<String, Integer>();
		Map<String, Double> tickerCI = new HashMap<String, Double>();
		Map<String, Double> tickerCF = new HashMap<String, Double>();
		Map<String, Integer> variazioneAnnuale = new HashMap<String, Integer>();

		long numberOfRecord = 0;
		long sumVolume = 0;
		double averageVolume = 0;
		int vaForTicker=0;
		int sumVa=0;
		
		

		for (Text value : values) {
			String[] input = value.toString().split(",");
			String ticker=input[TICKER];
			String giorno= input[DATE].split("-")[2];
			String mese= input[DATE].split("-")[1];
			double close= Double.parseDouble(input[CLOSE]);
			int date= Integer.parseInt(giorno+mese);
			long volume = Long.parseLong(input[VOLUME]);
			
			//numberOfRecord++;
		//	sumVolume += volume;
			
			if (tickervolume.containsKey(ticker)) {
				tickervolume.put(ticker, tickervolume.get(ticker)+volume);
			}
			else {
				tickervolume.put(ticker,volume);
			}

			
            if(tickerdatemin.containsKey(ticker)) {
				
				if (tickerdatemin.get(ticker) > date ) {
					tickerdatemin.replace(ticker, date);
					tickerCI.replace(ticker, close);
				}
				
				else {
					tickerdatemin.put(ticker,date);
					tickerCI.put(ticker, close);
				}
			}
				
				if(tickerdatemax.containsKey(ticker)) {
					
					if (tickerdatemax.get(ticker) < date ) {
						tickerdatemax.replace(ticker, date);
						tickerCF.replace(ticker, close);
					}
				}
					
					else {
						tickerdatemax.put(ticker,date);
						tickerCF.put(ticker, close);
					}
				
				
			
			
			
			
			
			
			
		}
		
		for (String ticker : tickerCF.keySet()) {
			
			double chiusuraIniziale= tickerCI.get(ticker);
			double chiusuraFinale= tickerCF.get(ticker);
			vaForTicker =  (int) Math.round(((chiusuraFinale - chiusuraIniziale) / chiusuraIniziale) * 100); 
			variazioneAnnuale.put(ticker, vaForTicker);
			
		}
		
		
		
		
		//float allVolumesSum = 0;
		for (long volume : tickervolume.values()) {
			sumVolume += volume;
		}
		double volumeAvg = sumVolume / (tickervolume.keySet().size());
		context.write(sectoryear, new Text(volumeAvg + ""));

	}

}
