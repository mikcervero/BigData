package BigData.Job1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;


public class JobOne {
	
	public static void main(String[] args) throws IOException {

        JobConf conf = new JobConf(JobOne.class);
        conf.setJobName("JobOne");
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
}

}