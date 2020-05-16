package job3;


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



public class JobThree {

	public static void main(String[] args) throws Exception {


	
//		if (args.length != 2) {
//			System.err.println("Usage: uniquelisteners <in> <out>");
//			System.exit(2);

		//---------------JOB 1--------------
		
		Configuration conf1 = new Configuration();
		
		Job job1 = new Job(conf1, "Job1");
		job1.setJarByClass(JobThree.class);

		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, StocksMapper.class);

		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, PricesMapper.class);

		job1.setReducerClass(JoinReducer.class);

		job1.setOutputKeyClass(Text.class);

		job1.setOutputValueClass(Text.class);
		
		Path joinoutput = new Path("output/joinoutput");

		FileOutputFormat.setOutputPath(job1, joinoutput);

		job1.waitForCompletion(true);

		
		//------------JOB 2---------------
		
		Configuration conf2 = new Configuration();
		
		Job job2 = new Job(conf2, "Job2");
		job2.setJarByClass(JobThree.class);
		
		FileInputFormat.addInputPath(job2, joinoutput);
		
		job2.setMapperClass(MapperTwo.class);
		
		job2.setOutputKeyClass(Text.class);
		
		job2.setOutputValueClass(Text.class);
		
		job2.setReducerClass(ReducerTwo.class);
		
		Path aziendetrend = new Path("output/aziendetrend");

		
		FileOutputFormat.setOutputPath(job2, aziendetrend);
		
		job2.waitForCompletion(true);
		
		
		//---------------JOB 3--------------
		
        Configuration conf3 = new Configuration();
		
		Job job3 = new Job(conf3, "Job3");
		job3.setJarByClass(JobThree.class);
		
		FileInputFormat.addInputPath(job3, aziendetrend);
		
		job3.setMapperClass(MapperThree.class);
		
		job3.setOutputKeyClass(Text.class);
		
		job3.setOutputValueClass(Text.class);
		
		job3.setReducerClass(ReducerThree.class);
		

		
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		
		job3.waitForCompletion(true);
		
		
		System.exit(0);
		
		
		org.apache.hadoop.mapreduce.Counters counters = job1.getCounters();

		System.out.println(
				"No. of Invalid Records :" + counters.findCounter(COUNTERS1.INVALID_RECORD_COUNT_JOB1).getValue() +" RIGHE INVALIDE MAPPER 1\t"
						+ counters.findCounter(COUNTERS2.INVALID_RECORD_COUNT_JOB2).getValue()+" RIGHE INVALIDE MAPPER 2");
	}

}
