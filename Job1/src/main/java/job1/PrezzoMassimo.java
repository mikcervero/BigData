package job1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrezzoMassimo {
	
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}
		
		
		
		Job job = new Job(conf, "MaxPrice");
		job.setJarByClass(PrezzoMassimo.class);
		
		job.setMapperClass(PrezzoMassimoMapper.class);
		
		job.setReducerClass(PrezzoMassimoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		
		System.out.println("No. of Invalid Records :"
				+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
						.getValue());
	}
	
    public static class PrezzoMassimoReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	

		public void reduce(Text ActionSymbolId,Iterable<FloatWritable> values,Context context)
				throws IOException, InterruptedException {
			
			float maxPrezzo= Float.MIN_VALUE;
			
			FloatWritable maxPriceforInterval= new FloatWritable();
			
			for (FloatWritable ActionMaxPrice : values) {
				
				float prezzoMassimo= Float.parseFloat(ActionMaxPrice.toString());
				
				if (prezzoMassimo<=maxPrezzo) {
					   maxPrezzo=prezzoMassimo;
				    }    
				
			 }
		    
			maxPriceforInterval.set(maxPrezzo);
			
			context.write(new Text(ActionSymbolId), maxPriceforInterval);
		}
	}
	
	
	public static class PrezzoMassimoMapper extends Mapper<Object, Text, Text, FloatWritable> {
  
	   private final int SYMBOL = 0;
       private final int PREZZOMASSIMO = 5;
       private final int DATE = 7;


       public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
	
	
	      String input = value.toString();
	      String[] campi= input.split(",");
	 
	
	      if(campi.length==8) {
		 
		       String[] Actiondate= campi[DATE].split("-");
		 
		       int anno= Integer.parseInt(Actiondate[0]); 
		 
		       FloatWritable prezzoMassimo= new FloatWritable();
	 
	           if(anno>=2008 && anno<=2018 ){
	    
	              prezzoMassimo.set(Float.parseFloat(campi[PREZZOMASSIMO]));	 

		          context.write(new Text(campi[SYMBOL]),prezzoMassimo);
	           }  
	      }
	 
	     else {
		   
		   context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
	     }
	 
	 

       }


    }
	
	


}
