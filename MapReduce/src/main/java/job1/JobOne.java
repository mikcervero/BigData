package job1;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobOne {

	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}

	public static void main(String[] args) throws Exception {
		
	
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "JobOne");
		job.setJarByClass(JobOne.class);

		job.setMapperClass(JobOneMapper.class);

		job.setReducerClass(JobOneReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();

		
		System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());

	}

	public static class JobOneReducer extends Reducer<Text, Text, Text, Text> {

		private final int PREZZOCHIUSURA = 0;
		private final int VOLUME = 1;
		private final int DATE = 2;
		private Map<Text, Text> mappa = new HashMap<Text, Text>();
		
		
		public void reduce(Text ActionSymbolId, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			double minPrezzo = Double.MAX_VALUE;
			double maxPrezzo = Double.MIN_VALUE;
			double sumVolume = 0;
			double prezzoChiusuraIniziale = 0;
			double prezzoChiusuraFinale = 0;
			long UltimaData = Long.MIN_VALUE;
			long PrimaData = Long.MAX_VALUE;
			double averageVolume = 0;
			double numberOfRecord = 0;
			int variazioneQuotazione = 0;
			
			for (Text Actionvalues : values) {
				String[] Avalue = Actionvalues.toString().split(",");
				long volume = Long.parseLong(Avalue[VOLUME]);
				long date = Long.parseLong(Avalue[DATE]);
				double prezzoChiusura = Double.parseDouble(Avalue[PREZZOCHIUSURA]);
				
				//conteggio record per media volume
				numberOfRecord++;
				
				//aggiorno il prezzo minimo
				if (prezzoChiusura <= minPrezzo) {
					minPrezzo = prezzoChiusura;
				}
				
				//aggiorno il prezzo massimo
				if (prezzoChiusura >= maxPrezzo) {
					maxPrezzo = prezzoChiusura;
				}

				sumVolume += volume;
				
				//aggiorno prezzo di chiusura iniziale
				if (date < PrimaData) {
					PrimaData = date;
					prezzoChiusuraIniziale = prezzoChiusura;
				}
				
				//agiorno prezzo di chiusura finale
				if (date > UltimaData) {
					UltimaData = date;
					prezzoChiusuraFinale = prezzoChiusura;
				}

			}
			
			//calcolo media volume
			averageVolume = sumVolume / numberOfRecord;
			
			//calcolo quotazione
			variazioneQuotazione = (int) Math
					.round(((prezzoChiusuraFinale - prezzoChiusuraIniziale) / prezzoChiusuraIniziale) * 100);

			Text result = new Text("    " +variazioneQuotazione+ "    " + minPrezzo + "    " + maxPrezzo + "    " + averageVolume);
			
			mappa.put(new Text(ActionSymbolId), result);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, Text> sortedMap = sortByValues(mappa);
			for (Text key : sortedMap.keySet()) {
				
				String[] result= sortedMap.get(key).toString().split("    ");
				
				Text value = new Text("    "+result[1]+"%     "+result[2]+"    "+result[3]+"    "+result[4]);
				
				context.write(key, value);
			}
			
		}
		
	}
	
	  //comparatore per ordinare i valori per quotazione decrescente
	  private static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
	        List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
	     
	        Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {

	           
	            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
	                return new Integer(o2.getValue().toString().split("    ")[1]).compareTo(new Integer(o1.getValue().toString().split( "    " )[1]));
	            }
	        });
	     

	        Map<K,V> sortedMap = new LinkedHashMap<K,V>();
	     
	        for(Map.Entry<K,V> entry: entries){
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }
	     
	        return sortedMap;
	    }

	

	public static class JobOneMapper extends Mapper<Object, Text, Text, Text> {

		private final int SYMBOL = 0;
		private final int PREZZOCHIUSURA = 2;
		private final int VOLUME = 6;
		private final int DATE = 7;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String input = value.toString();
			String[] campi = input.split(",");

			if (campi.length == 8) {

				String[] Actiondate = campi[DATE].split("-");

				int anno = Integer.parseInt(Actiondate[0]);

				if (anno >= 2008 && anno <= 2018) {

					long millisecondDate = transformDate(campi[DATE]);

					//chiave il ticker, valore chiusura, volume, data
					context.write(new Text(campi[SYMBOL]), new Text(campi[PREZZOCHIUSURA] + "," + campi[VOLUME] + "," + millisecondDate));
				}
			}

			else {
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
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
}
