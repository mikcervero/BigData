package BigData.Job1.VariazioneQuotazione;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VariazioneQuotazione {
	
	
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}
		
		
		
		Job job = new Job(conf, "VariazioneQuotazione");
		job.setJarByClass(VariazioneQuotazione.class);
		
		job.setMapperClass(VariazioneQuotazioneMapper.class);
		
		job.setReducerClass(VariazioneQuotazioneReducer.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		
		System.out.println("No. of Invalid Records :"
				+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
						.getValue());
	}

	public static class VariazioneQuotazioneReducer extends Reducer<Text, Text, Text, IntWritable> {
		
		private final int PREZZOCHIUSURA = 0;
		private final int DATE = 1; 

		public void reduce(Text ActionSymbolId, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			
			float prezzoChiusuraIniziale=0;
			float prezzoChiusuraFinale=0;
			long UltimaData=Long.MIN_VALUE;
			long PrimaData= Long.MAX_VALUE;
			int variazioneQuotazione=0;
			IntWritable variazioneQuotazioneIntervallo= new IntWritable();
			
			
			for (Text Actionvalues : values) {
				String[] Avalue= Actionvalues.toString().split(",");
				long date= Long.parseLong(Avalue[DATE]);
				float prezzoChiusura= Float.parseFloat(Avalue[PREZZOCHIUSURA]);
				
				 
				    if (date<PrimaData) {
				      PrimaData=date;
					  prezzoChiusuraIniziale=prezzoChiusura;
					}
				 
				    if (date>UltimaData) {
				       UltimaData=date;
					   prezzoChiusuraFinale=prezzoChiusura;
				    }
				
				
			      }
				
			variazioneQuotazione=Math.round(((prezzoChiusuraFinale - prezzoChiusuraIniziale )/prezzoChiusuraIniziale)*100);
			variazioneQuotazioneIntervallo.set(variazioneQuotazione);
			
			context.write(new Text(ActionSymbolId), variazioneQuotazioneIntervallo);
		}
	}

	public static class VariazioneQuotazioneMapper extends Mapper<Object, Text, Text, Text> {
          
		private final int SYMBOL = 0;
		private final int PREZZOCHIUSURA = 2;
		private final int DATE = 7; 
		
		

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			
			 String input = value.toString();
			 String[] campi= input.split(",");
			
			
			 if(campi.length==8) {
				 
			     String[] Actiondate= campi[DATE].split("-");
				 
				 int anno= Integer.parseInt(Actiondate[0]);
			 
			     if(anno>=2008 && anno<=2018 ){
				 
			       long millisecondDate= transformDate(campi[DATE]);

				   context.write(new Text(campi[SYMBOL]),new Text(campi[PREZZOCHIUSURA] + "," + millisecondDate ));
			     } 
			   }
			 
			 else {
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			 }
			 
			 

		}
		
		private long transformDate(String dataToTrasform ) {
			 SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd");
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
