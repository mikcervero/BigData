package job3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerTwo extends Reducer<Text, Text, Text, Text> {

	
	private final int CLOSE = 0;
	private final int DATE = 1;
	private final int ANNO = 2;
	
	// la chiave è azienda e ticker
	
	public void reduce(Text aziendaticker, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		

		// mappa contenente la data meno recente dell'anno, la chiave della mappa è l'anno  
		Map<String, Long> annodatemin = new HashMap<String, Long>();
		
		// mappa contenente la data più recente dell'anno, la chiave della mappa è l'anno 
		Map<String, Long> annodatemax = new HashMap<String, Long>();
		
		// mappa contenente il prezzo di chiusura inziale dell'anno, la chiave della mappa è l'anno 
		Map<String, Double> annoCI = new HashMap<String, Double>();
		
		// mappa contenente il prezzo di chiusura finale dell'anno, la chiave della mappa è l'anno 
		Map<String, Double> annoCF = new HashMap<String, Double>();
		
		// mappa contenente la variazione annuale, la chiave della mappa è l'anno
		Map<String, Integer> annoVariazione = new HashMap<String, Integer>();
		
		int vaForTicker=0;
		int primoValore=0;
		int secondoValore=0;
		int terzoValore=0;

		for (Text value : values) {
			String[] input = value.toString().split(",");
			String anno=input[ANNO];
			// trasformo la data dal formato year-month-day in millisecond per facilitare il confronto
			long millisecondDate = transformDate(input[DATE]);
			double close= Double.parseDouble(input[CLOSE]);
			

			// se la mappa contiene già come chiave l'anno passato come parametro
            if(annodatemin.containsKey(anno)) {
				
            	// se la data(valore) associata alla chiave è maggiore della data tratta da value, viene aggiornata 
				if (annodatemin.get(anno) > millisecondDate ) {
					
					annodatemin.put(anno, millisecondDate);
					// viene aggiornato anche il valore del prezzo di chiusura iniziale presente in value 
					annoCI.put(anno, close);
				}
				
            }
				
				else {
					// altrimenti inserisco la nuova chiave con la relativa data
					annodatemin.put(anno,millisecondDate);
					
					// altrimenti inserisco la nuova chiave con il relativo prezzo di chiusura
					annoCI.put(anno, close);
				}
			
				
				if(annodatemax.containsKey(anno)) {
					
					// se la data(valore) associata alla chiave è minore della data tratta da value, viene aggiornata 
					if (annodatemax.get(anno) < millisecondDate ) {
						annodatemax.put(anno, millisecondDate);
						annoCF.put(anno, close);
					}
				}
					
					else {
						annodatemax.put(anno,millisecondDate);
					   // viene aggiornato anche il valore del prezzo di chiusura finale presente in value 
						annoCF.put(anno, close);
					}
				
				
			
			
		}
		
		// per ogni anno calcolo la variazione e la inserisco in una mappa chiave valore 
		for (String anno : annoCF.keySet()) {
				
			double chiusuraIniziale= annoCI.get(anno);
			double chiusuraFinale=annoCF.get(anno);
			vaForTicker =  (int) Math.round(((chiusuraFinale - chiusuraIniziale) / chiusuraIniziale) * 100); 
			annoVariazione.put(anno, vaForTicker);
		
		}
		
		// se è presente un valore della variazione per i tre anni...
       
		if (annoVariazione.keySet().size()==3 ) {
			
			primoValore=annoVariazione.get("2016");
		    secondoValore=annoVariazione.get("2017");
		    terzoValore=annoVariazione.get("2018");
			
		context.write(new Text(aziendaticker+","), new Text(primoValore+","+ secondoValore+","+terzoValore));
		
		}
		
		
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
