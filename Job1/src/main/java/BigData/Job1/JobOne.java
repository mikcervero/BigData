package BigData.Job1;

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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobOne {

	private enum COUNTERS {
		INVALID_RECORD_COUNT
	}

	public static void main(String[] args) throws Exception {
		
		long start = System.currentTimeMillis();
		
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "JobOne");
		job.setJarByClass(JobOne.class);

		job.setNumReduceTasks(2);

		job.setMapperClass(JobOneMapper.class);

		job.setReducerClass(JobOneReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();

		long end = System.currentTimeMillis();

		NumberFormat formatter = new DecimalFormat("#0.000");

		System.out.println("Execution time is " + formatter.format((end - start) / 1000d / 60) + " min");

		System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());

	}

	public static class JobOneReducer extends Reducer<Text, Text, Text, Text> {

		private final int PREZZOCHIUSURA = 0;
		private final int PREZZOMINIMO = 1;
		private final int PREZZOMASSIMO = 2;
		private final int VOLUME = 3;
		private final int DATE = 4;

		public void reduce(Text ActionSymbolId, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			float minPrezzo = Float.MAX_VALUE;
			float maxPrezzo = Float.MIN_VALUE;
			long sumVolume = 0;
			float prezzoChiusuraIniziale = 0;
			float prezzoChiusuraFinale = 0;
			long UltimaData = Long.MIN_VALUE;
			long PrimaData = Long.MAX_VALUE;
			float averageVolume = 0;
			int numberOfRecord = 0;
			int variazioneQuotazione = 0;

			for (Text Actionvalues : values) {
				String[] Avalue = Actionvalues.toString().split(",");
				float prezzoMinimo = Float.parseFloat(Avalue[PREZZOMINIMO]);
				float prezzoMassimo = Float.parseFloat(Avalue[PREZZOMASSIMO]);
				long volume = Long.parseLong(Avalue[VOLUME]);
				long date = Long.parseLong(Avalue[DATE]);
				float prezzoChiusura = Float.parseFloat(Avalue[PREZZOCHIUSURA]);

				numberOfRecord++;

				if (prezzoMinimo <= minPrezzo) {
					minPrezzo = prezzoMinimo;
				}
				if (prezzoMassimo >= maxPrezzo) {
					maxPrezzo = prezzoMassimo;
				}

				sumVolume += volume;

				if (date < PrimaData) {
					PrimaData = date;
					prezzoChiusuraIniziale = prezzoChiusura;
				}

				if (date > UltimaData) {
					UltimaData = date;
					prezzoChiusuraFinale = prezzoChiusura;
				}

			}

			averageVolume = sumVolume / numberOfRecord;
			variazioneQuotazione = Math
					.round(((prezzoChiusuraFinale - prezzoChiusuraIniziale) / prezzoChiusuraIniziale) * 100);

			Text result = new Text(
					" " + variazioneQuotazione + " " + minPrezzo + " " + maxPrezzo + " " + averageVolume);
			context.write(new Text(ActionSymbolId), result);
		}
	}

	public static class JobOneMapper extends Mapper<Object, Text, Text, Text> {

		private final int SYMBOL = 0;
		private final int PREZZOCHIUSURA = 2;
		private final int PREZZOMINIMO = 4;
		private final int PREZZOMASSIMO = 5;
		private final int VOLUME = 6;
		private final int DATE = 7;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String input = value.toString();
			String[] campi = input.split(",");

			if (campi.length == 8) {

				String[] Actiondate = campi[DATE].split("-");

				int anno = Integer.parseInt(Actiondate[0]);

				if (anno >= 2008 && anno <= 2018) {

					long millisecondDate = transformDate(campi[DATE]);

					context.write(new Text(campi[SYMBOL]), new Text(campi[PREZZOCHIUSURA] + "," + campi[PREZZOMINIMO]
							+ "," + campi[PREZZOMASSIMO] + "," + campi[VOLUME] + "," + millisecondDate));
				}
			}

			else {
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			}

		}

		private long transformDate(String dataToTrasform) {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
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
