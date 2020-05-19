package job1;

import java.util.regex.Pattern;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class JobOneSpark {
	
	
	private static final Pattern DASH = Pattern.compile("-");

	public static void main(String[] args) {
		
		
		String file = "/home/fabiano/data//file/historical_stock_prices.csv";
		
		SparkSession spark = SparkSession
				.builder()
				.appName("JobOne")
				.getOrCreate();
				
		
		JavaRDD<String> line = spark.read().textFile(file).javaRDD();
		
		JavaRDD<String[]> words = line.map(s -> s.split(",")).filter(x -> x.length==8);
	
		//considero record con anni compresi tra 2008 e 2018
		JavaRDD<String[]> filtro = words.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2008 && (Integer.parseInt(DASH.split(x[7])[0])) <=2018));
	
		//(ticker  ->   chiusuramin, chiusuramax, chiusurainiziale, chiusurafinale, datainiziale, datafinale, volume, 1 (conteggio record))
		JavaPairRDD<String, Double[]> tupla = filtro.mapToPair(x -> new Tuple2<>(x[0],new Double[] {Double.parseDouble(x[2]),Double.parseDouble(x[2]), Double.parseDouble(x[2]),Double.parseDouble(x[2]), transformDate(x[7]), transformDate(x[7]), Double.parseDouble(x[6]), 1.0}));

		//(ticker -> min_close, max_close, chiusurainiziale, chiusurafinale, datainiziale, datafinale, somma_volume, conteggio record)
		JavaPairRDD<String, Double[]> agg = tupla.reduceByKey((x,y)-> {
			Double datainiziale ;
			Double chiusurainiziale;
			Double datafinale;
			Double chiusurafinale;
			if (x[4]<y[4]) {
				datainiziale = x[4];
				chiusurainiziale = x[2];
			}
			else {
				datainiziale = y[4];
				chiusurainiziale = y[2];
			}
			if (x[5]>y[5]) {
				datafinale = x[5];
				chiusurafinale = x[3];
			}
			else {
				datafinale = y[5];
				chiusurafinale = y[3];
			}
			return new Double[] {Math.min(x[0],y[0]), Math.max(x[1], y[1]),chiusurainiziale, chiusurafinale, datainiziale, datafinale,x[6]+y[6], x[7]+y[7]};
			});
		//ticker, quotazione, min_close, max_close, media volumi tutto ordinato per quotazione decrescente
		JavaRDD<String[]> ordinato = agg.map(couple -> new String [] {couple._1(), String.valueOf(Math.round((couple._2()[3]/couple._2()[2])*100-100)), String.valueOf(couple._2()[0]), String.valueOf(couple._2()[1]), String.valueOf((couple._2()[6])/(couple._2()[7]))}).sortBy(x -> Double.parseDouble(x[1]), false, 1);

		JavaRDD<String> risultato = ordinato.map(x -> x[0]+ ":"+ x[1]+ ","+ x[2]+ ","+ x[3]+ ","+ x[4]);

		risultato.saveAsTextFile("/home/fabiano/sparkresult.txt");
			
	
	}
	
	private static Double transformDate(String dataToTrasform ) {
		 SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd");
		 Date dateFrm = null; 
		 try {
		 dateFrm = format.parse(dataToTrasform); 
		 } catch (ParseException e) {
		 e.printStackTrace(); 
		 }
		 return Double.parseDouble(String.valueOf(dateFrm.getTime()));
	 }
	
}
