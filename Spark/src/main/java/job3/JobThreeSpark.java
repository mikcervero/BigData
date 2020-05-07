package job3;

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

public class JobThreeSpark {
	
	private static final Pattern COMMA = Pattern.compile(",");
	private static final Pattern DASH = Pattern.compile("-");
	
	public static void main(String[] args) {
		
		String file1 = "/home/fabiano/data/historical_stocks.csv";
		String file2 = "/home/fabiano/data/historical_stock_prices.csv";
		
		SparkSession spark = SparkSession
				.builder()
				.appName("JobTwo")
				.getOrCreate();
		
		JavaRDD<String> line1 = spark.read().textFile(file1).javaRDD();
		JavaRDD<String> line2 = spark.read().textFile(file2).javaRDD();
		
		JavaRDD<String[]> words1 = line1.map(s -> s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).filter(x -> x.length==5);
		JavaRDD<String[]> words2 = line2.map(s -> s.split(",")).filter(x -> x.length==8);

		
		JavaRDD<String[]> filtro1 = words1.filter(x -> !x[3].equals("N/A"));
		JavaRDD<String[]> filtro2 = words2.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2008 && (Integer.parseInt(DASH.split(x[7])[0])) <=2018));

		
		JavaPairRDD<String, String[]> stocks = filtro1.mapToPair(x-> new Tuple2<>(x[0], new String[] {x[1], x[2], x[3], x[4]}));
		JavaPairRDD<String, String[]> prices = filtro2.mapToPair(x-> new Tuple2<>(x[0], new String[] {x[1], x[2], x[3], x[4], x[5], x[6], x[7]}));
		
		JavaPairRDD<String, Tuple2<String[], String[]>> join = stocks.join(prices);
		JavaRDD<String> risultato = join.map(couple -> couple._1()+","+couple._2()._1[0]+","+couple._2()._1[1]+","+couple._2()._1[2]+","+couple._2()._1[3]+","+couple._2()._2[0]+","+couple._2()._2[1]+","+couple._2()._2[2]+","+couple._2()._2[3]+","+couple._2()._2[4]+","+couple._2()._2[5]+","+couple._2()._2[6]).coalesce(1);
	
		risultato.saveAsTextFile("/home/fabiano/risultato.txt");
		
	}

}
