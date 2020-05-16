package job2;

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



public class JobTwo {

	public static void main(String[] args) throws Exception {

		

		Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.err.println("Usage: uniquelisteners <in> <out>");
//			System.exit(2);

		//---------------JOB 1--------------
		
		Configuration conf1 = new Configuration();
		
		Job job1 = new Job(conf1, "Job1");
		job1.setJarByClass(JobTwo.class);

		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, StocksMapper.class);

		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, PricesMapper.class);

		job1.setReducerClass(JoinReducer.class);

		job1.setOutputKeyClass(Text.class);

		job1.setOutputValueClass(Text.class);
		
		Path job1output = new Path("output/job1output");

		FileOutputFormat.setOutputPath(job1, job1output);

		job1.waitForCompletion(true);

		
		//------------JOB 2---------------
		
		Configuration conf2 = new Configuration();
		
		Job job2 = new Job(conf, "Job2");
		job2.setJarByClass(JobTwo.class);
		
		FileInputFormat.addInputPath(job2, job1output);
		
		job2.setMapperClass(MapperTwo.class);
		
		job2.setOutputKeyClass(Text.class);
		
		job2.setOutputValueClass(Text.class);
		
		job2.setReducerClass(ReducerTwo.class);
		
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		job2.waitForCompletion(true);
		
		System.exit(0);
		
		org.apache.hadoop.mapreduce.Counters counters = job1.getCounters();

		
		System.out.println(
				"No. of Invalid Records :" + counters.findCounter(COUNTERS1.INVALID_RECORD_COUNT_JOB1).getValue() +" RIGHE INVALIDE MAPPER 1\t"
						+ counters.findCounter(COUNTERS2.INVALID_RECORD_COUNT_JOB2).getValue()+" RIGHE INVALIDE MAPPER 2");
	}

}
