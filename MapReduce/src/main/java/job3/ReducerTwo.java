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
	

	public void reduce(Text aziendaticker, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		Map<String, Long> annodatemin = new HashMap<String, Long>();
		Map<String, Long> annodatemax = new HashMap<String, Long>();
		Map<String, Double> annoCI = new HashMap<String, Double>();
		Map<String, Double> annoCF = new HashMap<String, Double>();
		Map<String, Integer> annoVariazione = new HashMap<String, Integer>();
		//String azienda= aziendaticker.toString().split(",")[0];
		//String ticker= aziendaticker.toString().split(",")[1];
		int vaForTicker=0;
		int primoValore=0;
		int secondoValore=0;
		int terzoValore=0;

		for (Text value : values) {
			String[] input = value.toString().split(",");
			String anno=input[ANNO];
			long millisecondDate = transformDate(input[DATE]);
			double close= Double.parseDouble(input[CLOSE]);
			

			
            if(annodatemin.containsKey(anno)) {
				
				if (annodatemin.get(anno) > millisecondDate ) {
					annodatemin.put(anno, millisecondDate);
					annoCI.put(anno, close);
				}
				
            }
				
				else {
					annodatemin.put(anno,millisecondDate);
					annoCI.put(anno, close);
				}
			
				
				if(annodatemax.containsKey(anno)) {
					
					if (annodatemax.get(anno) < millisecondDate ) {
						annodatemax.put(anno, millisecondDate);
						annoCF.put(anno, close);
					}
				}
					
					else {
						annodatemax.put(anno,millisecondDate);
						annoCF.put(anno, close);
					}
				
				
			
			
		}
		
		for (String anno : annoCF.keySet()) {
			
			
			double chiusuraIniziale= annoCI.get(anno);
			double chiusuraFinale=annoCF.get(anno);
			vaForTicker =  (int) Math.round(((chiusuraFinale - chiusuraIniziale) / chiusuraIniziale) * 100); 
			annoVariazione.put(anno, vaForTicker);
		
		}
       
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
