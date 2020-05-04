package job2;

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

import job2.MapperOne.COUNTERS1;
import job2.MapperTwo.COUNTERS2;



public class JobTwo {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();

		Configuration conf = new Configuration();
//		if (args.length != 2) {
//			System.err.println("Usage: uniquelisteners <in> <out>");
//			System.exit(2);

		Job job1 = new Job(conf, "Job1");
		job1.setJarByClass(JobTwo.class);

		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MapperOne.class);

		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MapperTwo.class);

		job1.setReducerClass(ReducerOne.class);

		job1.setOutputKeyClass(Text.class);

		job1.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		job1.waitForCompletion(true);

		org.apache.hadoop.mapreduce.Counters counters = job1.getCounters();

		long end = System.currentTimeMillis();

		NumberFormat formatter = new DecimalFormat("#0.000");

		System.out.println("Execution time is " + formatter.format((end - start) / 1000d / 60) + " min");

		System.out.println(
				"No. of Invalid Records :" + counters.findCounter(COUNTERS1.INVALID_RECORD_COUNT_JOB1).getValue()
						+ counters.findCounter(COUNTERS2.INVALID_RECORD_COUNT_JOB2).getValue());
	}

}
