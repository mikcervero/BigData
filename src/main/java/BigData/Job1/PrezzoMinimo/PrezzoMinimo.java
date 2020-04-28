package BigData.Job1.PrezzoMinimo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class PrezzoMinimo {
	
	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}
		
		
		
		Job job = new Job(conf, "MinPrice");
		job.setJarByClass(PrezzoMinimo.class);
		
		job.setMapperClass(PrezzoMinimoMapper.class);
		
		job.setReducerClass(PrezzoMinimoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		
		System.out.println("No. of Invalid Records :"
				+ counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
						.getValue());
	}
	
    public static class PrezzoMinimoReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	

		public void reduce(Text ActionSymbolId,Iterable<FloatWritable> values,Context context)
				throws IOException, InterruptedException {
			
			float minPrezzo= Float.MAX_VALUE;
			FloatWritable minPriceforInterval= new FloatWritable();
			
			for (FloatWritable ActionMinPrice : values) {
				
				float prezzoMinimo= Float.parseFloat(ActionMinPrice.toString());
				
				if (prezzoMinimo<=minPrezzo) {
					   minPrezzo=prezzoMinimo;
				    }    
				
			 }
		    
			minPriceforInterval.set(minPrezzo);
			context.write(new Text(ActionSymbolId), minPriceforInterval);
		}
	}
	
	
	public static class PrezzoMinimoMapper extends Mapper<Object, Text, Text, FloatWritable> {
  
	   private final int SYMBOL = 0;
       private final int PREZZOMINIMO = 4;
       private final int DATE = 7;


       public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
	
	
	      String input = value.toString();
	      String[] campi= input.split(",");
	 
	
	      if(campi.length==8) {
		 
		       String[] Actiondate= campi[DATE].split("-");
		 
		       int anno= Integer.parseInt(Actiondate[0]); 
		 
		       FloatWritable prezzoMinimo= new FloatWritable();
	 
	           if(anno>=2008 && anno<=2018 ){
	    
	              prezzoMinimo.set(Float.parseFloat(campi[PREZZOMINIMO]));	 

		          context.write(new Text(campi[SYMBOL]),prezzoMinimo);
	           }  
	      }
	 
	     else {
		   
		   context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
	     }
	 
	 

       }


    }
	
	

}
