package BigData.Job1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		
		System.out.println("No. of Invalid Records :"
				+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
						.getValue());
	}

	public static class JobOneReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(
				Text ActionSymbolId,
				Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			float minPrezzo= Float.MAX_VALUE;
			float maxPrezzo= Float.MAX_VALUE;
			long sumVolume=0;
			float prezzoChiusuraIniziale=0;
			float prezzoChiusuraFinale=0;
			long UltimaData=0;
			long PrimaData=0;
			float averageVolume=0;
			int numberOfRecord=0;
			int variazioneQuotazione=0;
			

			
			for (Text Actionvalues : values) {
				String[] Avalue= Actionvalues.toString().split(",");
				float prezzoMinimo= Float.parseFloat(Avalue[4]);
				float prezzoMassimo= Float.parseFloat(Avalue[5]);
				long volume=Long.parseLong(Avalue[6]);
				long date= Long.parseLong(Avalue[7]);
				float prezzoChiusura= Float.parseFloat(Avalue[2]);
				
				numberOfRecord++;
				
				if (prezzoMinimo<=minPrezzo) {
					minPrezzo=prezzoMinimo;
				 }
				if (prezzoMassimo>=maxPrezzo) {
					maxPrezzo=prezzoMassimo;
				 }
				
				 sumVolume+=volume;
				 
				 if (date<=PrimaData) {
					 prezzoChiusuraIniziale=prezzoChiusura;
					 }
				 
				 if (date>=UltimaData) {
					 prezzoChiusuraFinale=prezzoChiusura;
					 }
				
				
			}
			
			averageVolume= sumVolume/numberOfRecord;
			variazioneQuotazione=Math.round(((prezzoChiusuraFinale - prezzoChiusuraIniziale )/prezzoChiusuraIniziale)*100);
			
			Text result= new Text(variazioneQuotazione + " " + minPrezzo + " " + maxPrezzo + " " + averageVolume );
			
			
			
			context.write(new Text(ActionSymbolId), result);
		}
	}

	public static class JobOneMapper extends
			Mapper<Object, Text, Text, Text> {
          
		private final int SYMBOL = 0;
		private final int PREZZOCHIUSURA = 2;
		private final int PREZZOMINIMO = 4;
		private final int PREZZOMASSIMO = 5;
		private final int VOLUME = 6;
		private final int DATE = 7; 
		
		

		public void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			 String input = value.toString();
			 String[] campi= input.split(",");
			 String[] Actiondate= campi[DATE].split("-");
			 
			 System.out.println("DATA= "+ campi[7]);
			 
			 //problema sta qui
			 int anno= Integer.parseInt(Actiondate[0]);
			
			 if(campi.length==8) {
			 
			   if(anno>=2008 && anno<=2018 ){
				 
			     long millisecondDate= transformDate(campi[DATE]);

				 context.write(new Text(campi[SYMBOL]),new Text(campi[PREZZOCHIUSURA] + "," + campi[PREZZOMINIMO] + "," + campi[PREZZOMASSIMO] + ","+ campi[VOLUME] + "," + millisecondDate));
			   } 
			 }else {
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


