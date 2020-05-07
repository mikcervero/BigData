package job3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import Parser.Parser;
import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Sets;
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
		JavaRDD<String[]> filtro2 = words2.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2016));

		
		JavaPairRDD<String, String[]> stocks = filtro1.mapToPair(x-> new Tuple2<>(x[0], new String[] {x[2]}));
		//chiusura, data, anno
		JavaPairRDD<String, String[]> prices = filtro2.mapToPair(x-> new Tuple2<>(x[0], new String[] {x[2], x[7], DASH.split(x[7])[0]}));
		
		JavaPairRDD<String, Tuple2<String[], String[]>> join = stocks.join(prices);
	
		//nome, chiusura, data, anno
		JavaRDD<String[]> joinresult = join.map(couple -> new String[] { couple._2()._1[0], couple._2()._2[0], couple._2()._2[1], couple._2()._2[2]});
		JavaPairRDD<String , Double[]> tupla = joinresult.mapToPair(x -> new Tuple2<>(x[0]+","+ x[3], new Double[] {Double.parseDouble(x[1]), transformDate(x[2])}));
		
		JavaPairRDD<String, Double[]> agg = tupla.reduceByKey((x,y) -> new Double[] {chiusurainiziale(x[0], y[0], x[1], y[1]), chiusurafinale(x[0], y[0], x[1], y[1])});
		
		JavaRDD<String[]> intermediate = agg.map(couple -> new String[] {couple._1().split(",")[0],couple._1().split(",")[1],String.valueOf(Math.round((couple._2()[1]/couple._2()[0])*100-100))} );
		
		
		JavaPairRDD<String, Iterable<String>> intermediate2 =  intermediate.mapToPair(x -> new Tuple2<>(x[1]+","+x[2], x[0])).groupByKey();
		
		
		JavaRDD<String> risultato = intermediate2.map(couple -> couple._1()+","+Sets.newHashSet(couple._2())).coalesce(1);
				
		risultato.saveAsTextFile("/home/fabiano/risultato.txt");
		
	}
	
	public static Double chiusurainiziale(Double oldclose, Double newclose, Double olddate, Double newdate) {
		if (newdate < olddate) {
			return newclose;
		}
		return oldclose;
	}

	public static Double chiusurafinale(Double oldclose, Double newclose, Double olddate, Double newdate) {
		if (newdate > olddate) {
			return newclose;
		}
		return oldclose;
	}

	public static Double transformDate(String dataToTrasform) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date dateFrm = null;
		try {
			dateFrm = format.parse(dataToTrasform);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return Double.parseDouble(String.valueOf(dateFrm.getTime()));
	}

}
