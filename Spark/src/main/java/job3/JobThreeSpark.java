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
	private static final int TICKER = 0;
	private static final int NAME = 2;

	public static void main(String[] args) {
		
		String file1 = "/home/fabiano/data/historical_stocks.csv";
		String file2 = "/home/fabiano/data/historical_stock_prices.csv";

		SparkSession spark = SparkSession
				.builder()
				.appName("JobThree")
				.getOrCreate();
		
		JavaRDD<String> line1 = spark.read().textFile(file1).javaRDD();
		JavaRDD<String> line2 = spark.read().textFile(file2).javaRDD();
		
		JavaRDD<String[]> words1 = line1.map(s -> processString(s)).filter(x -> x!=null).map(x->new String[] {x.split(",")[0], x.split(",")[1]});
		JavaRDD<String[]> words2 = line2.map(s -> s.split(",")).filter(x -> x.length==8);

		JavaRDD<String[]> filtro2 = words2.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2016));

		//nome
		JavaPairRDD<String, String[]> stocks = words1.mapToPair(x-> new Tuple2<>(x[0], new String[] {x[1]}));
		//chiusura, data, anno
		JavaPairRDD<String, String[]> prices = filtro2.mapToPair(x-> new Tuple2<>(x[0], new String[] {x[2], x[7], DASH.split(x[7])[0]}));
		//ticker,nome,chiusura, data, anno
		JavaPairRDD<String, Tuple2<String[], String[]>> join = stocks.join(prices);
	
		//ticker, nome, chiusura, data, anno      
		JavaRDD<String[]> joinresult = join.map(couple -> new String[] { couple._1(), couple._2()._1[0], couple._2()._2[0], couple._2()._2[1], couple._2()._2[2]});
	
		//(ticker, nome, anno   ->       chiusura data)
		JavaPairRDD<String , Double[]> tupla = joinresult.mapToPair(x -> new Tuple2<>(x[0]+","+ x[1]+","+x[4], new Double[] {Double.parseDouble(x[2]), transformDate(x[3])}));
		
		//(ticker, nome, anno    ->     chiusuriniziale, chiusurafinale)    
		JavaPairRDD<String, Double[]> agg = tupla.reduceByKey((x,y) -> new Double[] {chiusurainiziale(x[0], y[0], x[1], y[1]), chiusurafinale(x[0], y[0], x[1], y[1])});
	
		
			
	    //ticker,nome,anno,quotazione   
		JavaRDD<String[]> risultato = agg.map(couple -> new String[] {couple._1().split(",")[0], couple._1().split(",")[1], couple._1().split(",")[2], String.valueOf(Math.round((couple._2()[1]/couple._2()[0])*100-100))}).sortBy(x->Long.parseLong(x[2]), true, 1);
	
		//prima groupBy per ogni ticker e nome ho le quotazioni, seconda groupBy per ogni quotazione ho le aziende che hanno avuto stesso trend
		 JavaPairRDD<Iterable<String>, Iterable<String>> quotazioni = risultato.mapToPair(x -> new Tuple2<>(x[0]+","+x[1], x[3])).groupByKey().mapToPair(couple->new Tuple2<>(couple._2(), couple._1())).groupByKey().coalesce(1);                   
		
				
		quotazioni.saveAsTextFile("/home/fabiano/risultato.txt");
		
	}
	
	  public static String processString (String text)	{
		  
		    String[] fields= text.split(",");
			String ticker= fields[TICKER];
			String sector= fields[fields.length-2];
			String industry=fields[fields.length-1];
			String result = null;
			
			
			
	        if(industry.indexOf('"')!=-1) {
				
				sector= fields[fields.length-3];
			}
			
			
			
			
			if(sector.equals("N/A")) {
				
				return result; 
			}
			
			
			
					
			if(fields[NAME].indexOf('"')!=-1) {
				fields[NAME]=fields[2].replace('"', ' ');
				fields[3]=fields[3].replace('"', ' ');
				fields[NAME]=fields[2]+fields[3];
		        
			}
			
			
					
				 return result= ticker + "," + fields[NAME] + "," + sector ;
				 	
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
