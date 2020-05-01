package BigData.Job1.VolumeMedio;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import BigData.Job1.PrezzoMinimo.PrezzoMinimo;

public class VolumeMedio {
	
	
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}
		
		
		
		Job job = new Job(conf, "AverageVolume");
		job.setJarByClass(PrezzoMinimo.class);
		
		job.setMapperClass(VolumeMedioMapper.class);
		
		job.setReducerClass(VolumeMedioReducer.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		
		System.out.println("No. of Invalid Records :"
				+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
						.getValue());
	}
	
    public static class VolumeMedioReducer extends Reducer<Text, LongWritable, Text, FloatWritable> {
	

		public void reduce(Text ActionSymbolId,Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
			
			float averageVolume=0;
			int numberOfRecord=0;
			long sumVolume=0;
			FloatWritable averageVolumeForInterval= new FloatWritable();
			
			for (LongWritable ActionVolume : values) {
				
				numberOfRecord++;
				
				sumVolume += Long.parseLong(ActionVolume.toString());
				
				
			 }
		    
			averageVolume= sumVolume/numberOfRecord;
			
			averageVolumeForInterval.set(averageVolume);
			
			context.write(new Text(ActionSymbolId), averageVolumeForInterval);
		}
	}
	
	
	public static class VolumeMedioMapper extends Mapper<Object, Text, Text, LongWritable> {
  
	   private final int SYMBOL = 0;
	   private final int VOLUME = 6;
       private final int DATE = 7;


       public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
	
	
	      String input = value.toString();
	      String[] campi= input.split(",");
	 
	
	      if(campi.length==8) {
		 
		       String[] Actiondate= campi[DATE].split("-");
		 
		       int anno= Integer.parseInt(Actiondate[0]); 
		 
		       LongWritable volume= new LongWritable();
	 
	           if(anno>=2008 && anno<=2018 ){
	    
	              volume.set(Long.parseLong(campi[VOLUME]));	 

		          context.write(new Text(campi[SYMBOL]),volume);
	           }  
	      }
	 
	     else {
		   
		   context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
	     }
	 
	 

       }


    }

}
