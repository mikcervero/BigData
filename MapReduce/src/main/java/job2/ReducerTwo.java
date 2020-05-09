package job2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerTwo extends Reducer<Text, Text, Text, Text> {

	private final int TICKER = 0;
	private final int VOLUME = 1;
	private final int CLOSE = 2;
	private final int DATE = 3;
	
	

	public void reduce(Text sectoryear, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Long> tickervolume = new HashMap<String, Long>();
		Map<String, Long> tickerdatemin = new HashMap<String, Long>();
		Map<String, Long> tickerdatemax = new HashMap<String, Long>();
		Map<String, Double> tickerCI = new HashMap<String, Double>();
		Map<String, Double> tickerCF = new HashMap<String, Double>();
		Map<String, Integer> variazioneAnnuale = new HashMap<String, Integer>();
		Map<String, Double> tickersumclose = new HashMap<String, Double>();
		Map<String, Integer> quantiCloseInTicker = new HashMap<String, Integer>();
		Map<String, Double> tickerclosemedia = new HashMap<String, Double>();

		long numberOfRecord = 0;
		long sumVolume = 0;
		double averageVolume = 0;
		int vaForTicker=0;
		int sumVa=0;
		double averageVa=0;
		double averageClose=0;
		double SumClose=0;
		
		

		for (Text value : values) {
			String[] input = value.toString().split(",");
			String ticker=input[TICKER];
			long millisecondDate = transformDate(input[DATE]);
			double close= Double.parseDouble(input[CLOSE]);
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
				
				if (tickerdatemin.get(ticker) > millisecondDate ) {
					tickerdatemin.put(ticker, millisecondDate);
					tickerCI.put(ticker, close);
				}
				
            }
				
				else {
					tickerdatemin.put(ticker,millisecondDate);
					tickerCI.put(ticker, close);
				}
			
				
				if(tickerdatemax.containsKey(ticker)) {
					
					if (tickerdatemax.get(ticker) < millisecondDate ) {
						tickerdatemax.put(ticker, millisecondDate);
						tickerCF.put(ticker, close);
					}
				}
					
					else {
						tickerdatemax.put(ticker,millisecondDate);
						tickerCF.put(ticker, close);
					}
				
				
				if (tickersumclose.containsKey(ticker)) {
					tickersumclose.put(ticker, tickersumclose.get(ticker)+close);
					quantiCloseInTicker.put(ticker,quantiCloseInTicker.get(ticker)+1);
				}
				else {
					tickersumclose.put(ticker,close);
					quantiCloseInTicker.put(ticker,1);
				}
			
			
		}
		
		for (String ticker : tickerCF.keySet()) {
			
			double chiusuraIniziale= tickerCI.get(ticker);
			double chiusuraFinale= tickerCF.get(ticker);
			vaForTicker =  (int) Math.round(((chiusuraFinale - chiusuraIniziale) / chiusuraIniziale) * 100); 
			variazioneAnnuale.put(ticker, vaForTicker);
			
		}
		
		for (int varazione: variazioneAnnuale.values()) {
			
			sumVa+=varazione;
			
		}
		
		
		averageVa= sumVa/(variazioneAnnuale.keySet().size());
		
		
        for (String ticker : tickersumclose.keySet()) {
        	
        	double tickerCloseAverage= tickersumclose.get(ticker)/quantiCloseInTicker.get(ticker);
			
        	tickerclosemedia.put(ticker,tickerCloseAverage);
			
		}
        
        for(double closeMedioTicker: tickerclosemedia.values()) {
        	SumClose+=closeMedioTicker;
        }
        
        
        averageClose= SumClose/(tickerclosemedia.keySet().size());
		
		
		//float allVolumesSum = 0;
		for (long volume : tickervolume.values()) {
			sumVolume += volume;
		}
		double volumeAvg = sumVolume / (tickervolume.keySet().size());
		context.write(sectoryear, new Text( "    " +volumeAvg + "    " + averageVa+"    "+ averageClose));

	}
	
	private long transformDate(String dataToTrasform) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date dateFrm = null;
		try {
			dateFrm = format.parse(dataToTrasform);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dateFrm.getTime();
	}

}
