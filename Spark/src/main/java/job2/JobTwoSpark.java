package job2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import Parser.Parser;
import scala.Tuple2;

public class JobTwoSpark {
	
	private static final Pattern COMMA = Pattern.compile(",");
	private static final Pattern DASH = Pattern.compile("-");
	
	public static void main(String[] args) {
		
		Parser parser= new Parser();
		List<String> rowParse= new ArrayList<String>();
		
		SparkSession spark = SparkSession
				.builder()
				.appName("JobTwo")
				.getOrCreate();
		
		
		
		Dataset<String> historical_stocks= spark.read().textFile(args[1]);
		
		List<String> rowNoParse=historical_stocks.collectAsList();
		
		for (String row : rowNoParse) {
			rowParse.add(parser.processString(row));
		}
		
		Dataset<String> historical_stocks_parse= spark.createDataset(rowParse, Encoders.STRING());
		
	
		
		JavaRDD<String> historical_stock_prices = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String[]> wordHistorical_stock_prices = historical_stock_prices.map(s -> COMMA.split(s)).filter(x -> x.length==8);
		JavaRDD<String[]> filtro = wordHistorical_stock_prices.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2008 && (Integer.parseInt(DASH.split(x[7])[0])) <=2018));
		JavaPairRDD<String,String[]> keyValue_historical_stock_prices = filtro.mapToPair(line -> new Tuple2<>(line[0], new String[] {line[2],line[6],line[7]}));
		
		JavaRDD<String> stocks_parse = historical_stocks_parse.javaRDD();
		JavaRDD<String[]>wordStocks_parse= stocks_parse.map(s -> COMMA.split(s)).filter(x -> x.length==3);
		JavaPairRDD<String,String[]> keyValue_stocks_parse= wordStocks_parse.mapToPair(line -> new Tuple2<>(line[0], new String[] {line[1],line[2]}));
		
		JavaPairRDD<String, Tuple2<String[],String[]>> risultato=keyValue_historical_stock_prices.join(keyValue_stocks_parse);
		
		//JavaPairRDD<String,String[]>risultato= datasetJoin.
		
		risultato.saveAsTextFile("/home/micol/sparkresult.txt");
		
		
		//JavaRDD<String[]> wordsSecond = secondDataset.map(s -> COMMA.split(s)).filter(x -> x.length==3);
		
		
		spark.stop();
		
		
		
		
		
		
		
		
		
		
		
	}

}
