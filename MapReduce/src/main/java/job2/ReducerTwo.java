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

		
		// mappa contenente la somma di tutti i volumi di un ticker nell'anno, chiave ticker value somma
		Map<String, Long> tickervolume = new HashMap<String, Long>();
		
		// mappa contenente come valori la data più remota di un ticker nell'anno e come chiave il ticker 
		Map<String, Long> tickerdatemin = new HashMap<String, Long>();
		
		// mappa contenente come valori la data più recente di un ticker nell'anno e come chiave il ticker 
		Map<String, Long> tickerdatemax = new HashMap<String, Long>();
		
		// mappa contenente come valori ivprezzi di chiusura di un ticker relativo alla data meno recente nell'anno e come chiave il ticker
		Map<String, Double> tickerCI = new HashMap<String, Double>();
		
		// mappa contenente come valori i prezzi di chiusura di un ticker relativo alla data più recente nell'anno e come chiave il ticker
		Map<String, Double> tickerCF = new HashMap<String, Double>();
		
		// mappa contente la variazione annula relativa ad ogni ticker
		Map<String, Integer> variazioneAnnuale = new HashMap<String, Integer>();
		
		// mappa contente la somma di tutti i valori prezzi di chiusura nell'anno di un ticker
		Map<String, Double> tickersumclose = new HashMap<String, Double>();
		
		// mappa contente il numero dei prezzi di chiusura di un ticker nell'anno
		Map<String, Integer> quantiCloseInTicker = new HashMap<String, Integer>();
		
		// mappa contente il prezzo di chiusura medio di un ticker nell'anno
		Map<String, Double> tickerclosemedia = new HashMap<String, Double>();

		long numberOfRecord = 0;
		long sumVolume = 0;
		double averageVolume = 0;
		int vaForTicker=0;
		double sumVa=0;
		double averageVa=0;
		double averageClose=0;
		double SumClose=0;
		
		

		for (Text value : values) {
			String[] input = value.toString().split(",");
			String ticker=input[TICKER];
			// trasformo la data dal formato year-month-day in millisecond per facilitare il confronto
			long millisecondDate = transformDate(input[DATE]);
			double close= Double.parseDouble(input[CLOSE]);
			long volume = Long.parseLong(input[VOLUME]);
		
            // se la mappa tickervolume contiene la chiave ticker, sommo al valore, relativo alla chiave, il volume tratto da value 
			
			if (tickervolume.containsKey(ticker)) {
				tickervolume.put(ticker, tickervolume.get(ticker)+volume);
			}
			
			// altrimenti inserisco la nuova chiave con il relativo valore 
			else {
				tickervolume.put(ticker,volume);
			}

			// se la mappa contiene già come chiave il ticker passato come parametro
            if(tickerdatemin.containsKey(ticker)) {
            	
				// se la data(valore) associata alla chiave ticker è maggiore della data tratta da value, viene aggiornata 
				if (tickerdatemin.get(ticker) > millisecondDate ) {
					tickerdatemin.put(ticker, millisecondDate);
					// viene aggiornato anche il valore del prezzo di chiusura iniziale presente in value 
					tickerCI.put(ticker, close);
				}
				
            }
            
				else {
					// altrimenti inserisco la nuova chiave con la relativa data
					tickerdatemin.put(ticker,millisecondDate);
					
					// altrimenti inserisco la nuova chiave con il relativo prezzo di chiusura
					tickerCI.put(ticker, close);
				}
			
                // se la mappa contiene già come chiave il ticker passato come parametro
				if(tickerdatemax.containsKey(ticker)) {
					
					// se la data(valore) associata alla chiave ticker è minore della data tratta da value, viene aggiornata 
					if (tickerdatemax.get(ticker) < millisecondDate ) {
						tickerdatemax.put(ticker, millisecondDate);
						
						// viene aggiornato anche il valore del prezzo di chiusura finale presente in value 
						tickerCF.put(ticker, close);
					}
				}
					
					else {
						// altrimenti inserisco la nuova chiave con la relativa data
						tickerdatemax.put(ticker,millisecondDate);
						
						// altrimenti inserisco la nuova chiave con il relativo prezzo di chiusura
						tickerCF.put(ticker, close);
					}
				
				// se la mappa contiene già come chiave il ticker passato come parametro
				if (tickersumclose.containsKey(ticker)) {
					
					//sommo al valore close , relativo alla chiave, il prezzo di chiusura tratto da value
					tickersumclose.put(ticker, tickersumclose.get(ticker)+close);
					
					//aggiorno il valore della mappa che ad un ticker associa il numero dei prezzi di chiusura presenti nell'anno
					quantiCloseInTicker.put(ticker,quantiCloseInTicker.get(ticker)+1);
				}
				
				else {
					// altrimenti inserisco la nuova chiave con il relativo prezzo di chiusura 
					tickersumclose.put(ticker,close);
					
					// altrimenti inserisco la nuova chiave e come valore inserisco 1
					quantiCloseInTicker.put(ticker,1);
				}
			
			
		}
		
		// per ogni ticker mi calcolo la variazione nell'anno e lo inserisco in una mappa 
		for (String ticker : tickerCF.keySet()) {
			
			double chiusuraIniziale= tickerCI.get(ticker);
			double chiusuraFinale= tickerCF.get(ticker);
			vaForTicker =  (int) Math.round(((chiusuraFinale - chiusuraIniziale) / chiusuraIniziale) * 100); 
			variazioneAnnuale.put(ticker, vaForTicker);
			
		}
		
		
		// sommo tutte le variazioni(valori) presenti nella mappa per poi poter calcolare la media
		
		for (int varazione: variazioneAnnuale.values()) {
			
			sumVa+=varazione;
			
		}
		
		
		averageVa= sumVa/(variazioneAnnuale.keySet().size());
		
		
		// per ogni ticker calcolo la media dei prezzi di chiusura nell'anno e la inserisco in una mappa con chiave ticker
		
        for (String ticker : tickersumclose.keySet()) {
        	
        	double tickerCloseAverage= tickersumclose.get(ticker)/quantiCloseInTicker.get(ticker);
			
        	tickerclosemedia.put(ticker,tickerCloseAverage);
			
		}
        
        // sommo i prezzi di chiusura medi di tutti ticker nell'anno 
        
        for(double closeMedioTicker: tickerclosemedia.values()) {
        	SumClose+=closeMedioTicker;
        }
        
        // e ne calcolo la media
        averageClose= SumClose/(tickerclosemedia.keySet().size());
		
		
		// effettuo una somma di tutti i valori presenti nella mappa
        
		for (long volume : tickervolume.values()) {
			sumVolume += volume;
		}
		// e ne calcolo la media
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
