package job3;



import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import job2.StocksMapper.COUNTERS1;
import job2.PricesMapper.COUNTERS2;



public class JobTrhee {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();

		Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.err.println("Usage: uniquelisteners <in> <out>");
//			System.exit(2);

		//---------------JOB 1--------------
		
		
		Job job1 = new Job(conf, "Job1");
		job1.setJarByClass(JobTrhee.class);

		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, StocksMapper.class);

		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, PricesMapper.class);

		job1.setReducerClass(JoinReducer.class);

		job1.setOutputKeyClass(Text.class);

		job1.setOutputValueClass(Text.class);
		
		Path joinoutput = new Path("output/joinoutput");

		FileOutputFormat.setOutputPath(job1, joinoutput);

		job1.waitForCompletion(true);

		
		//------------JOB 2---------------
		
		Job job2 = new Job(conf, "Job2");
		job2.setJarByClass(JobTrhee.class);
		
		FileInputFormat.addInputPath(job2, joinoutput);
		
		job2.setMapperClass(MapperTwo.class);
		
		job2.setOutputKeyClass(Text.class);
		
		job2.setOutputValueClass(Text.class);
		
		job2.setReducerClass(ReducerTwo.class);
		
		Path aziendetrend = new Path("output/aziendetrend");

		
		FileOutputFormat.setOutputPath(job2, aziendetrend);
		
		job2.waitForCompletion(true);
		
		
		//---------------JOB 3--------------
		
		Job job3 = new Job(conf, "Job3");
		job3.setJarByClass(JobTrhee.class);
		
		FileInputFormat.addInputPath(job3, aziendetrend);
		
		job2.setMapperClass(MapperTrhee.class);
		
		job2.setOutputKeyClass(Text.class);
		
		job2.setOutputValueClass(Text.class);
		
		job2.setReducerClass(ReducerTrhee.class);
		
		

		
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		job2.waitForCompletion(true);
		
		
		
		
		
		org.apache.hadoop.mapreduce.Counters counters = job1.getCounters();

		long end = System.currentTimeMillis();

		NumberFormat formatter = new DecimalFormat("#0.000");

		System.out.println("Execution time is " + formatter.format((end - start) / 1000d / 60) + " min");

		System.out.println(
				"No. of Invalid Records :" + counters.findCounter(COUNTERS1.INVALID_RECORD_COUNT_JOB1).getValue() +" RIGHE INVALIDE MAPPER 1\t"
						+ counters.findCounter(COUNTERS2.INVALID_RECORD_COUNT_JOB2).getValue()+" RIGHE INVALIDE MAPPER 2");
	}

}
