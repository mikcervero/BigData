package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	private final int SECTOR = 1;
	private final int NAME = 2;
	private final int PREZZOCHIUSURA = 1;
	private final int VOLUME = 2;
	private final int DATE = 3;

	public void reduce(Text ActionSymbolId, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String[]> stocks = new ArrayList<String[]>();
		List<String[]> prices = new ArrayList<String[]>();

		for (Text value : values) {
			String[] campi = value.toString().split(",");

			String id = campi[0];

			if (id.equals("stocks")) {
				stocks.add(campi);
			}

			else {
				prices.add(campi);
			}
		}

		for (String[] stock : stocks) {

			for (String[] price : prices) {
				
				context.write(new Text(ActionSymbolId+","), new Text(stock[SECTOR]+","+stock[NAME]+","+price[VOLUME]+","+price[PREZZOCHIUSURA]+","+price[DATE]));
			}
		}
	}

}
