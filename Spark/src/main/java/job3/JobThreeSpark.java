package job3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


import avro.shaded.com.google.common.collect.Lists;
import avro.shaded.com.google.common.collect.Sets;
import scala.Tuple2;
import scala.Tuple3;

public class JobThreeSpark {

	private static final Pattern COMMA = Pattern.compile(",");
	private static final Pattern DASH = Pattern.compile("-");
	private static final int TICKER = 0;
	private static final int NAME = 2;
	private static final int EXCHANGE = 1;

	public static void main(String[] args) {
		

		SparkSession spark = SparkSession
				.builder()
				.appName("JobThree")
				.getOrCreate();
		
		JavaRDD<String> line1 = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> line2 = spark.read().textFile(args[1]).javaRDD();
		
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
	
		//((ticker, nome, anno)   ->       chiusurainiziale, chiusurafinale, datainiziale, datafinale)
		JavaPairRDD<Tuple3<String,String,String> , Double[]> tupla = joinresult.mapToPair(x -> new Tuple2<>(new Tuple3<>(x[0],x[1],x[4]), new Double[] {Double.parseDouble(x[2]), Double.parseDouble(x[2]), transformDate(x[3]), transformDate(x[3])}));
		
		//((ticker, nome, anno)    ->     chiusuriniziale, chiusurafinale, datainiziale, datafinale)    
		JavaPairRDD<Tuple3<String,String,String> , Double[]> agg = tupla.reduceByKey((x,y) -> {
			Double datainiziale ;
			Double chiusurainiziale;
			Double datafinale;
			Double chiusurafinale;
			if (x[2]<y[2]) {
				datainiziale = x[2];
				chiusurainiziale = x[0];
			}
			else {
				datainiziale = y[2];
				chiusurainiziale = y[0];
			}
			if (x[3]>y[3]) {
				datafinale = x[3];
				chiusurafinale = x[1];
			}
			else {
				datafinale = y[3];
				chiusurafinale = y[1];
			}	
			
			
			return new Double[] { chiusurainiziale, chiusurafinale, datainiziale, datafinale};
		});
			
	    //ticker,nome,anno,quotazione   
		JavaRDD<String[]> risultato = agg.map(couple -> new String[] {couple._1()._1(), couple._1()._2(), couple._1()._3(), String.valueOf(Math.round((couple._2()[1]/couple._2()[0])*100-100))}).sortBy(x->Long.parseLong(x[2]), true, 1);
	
		//prima groupBy per ogni ticker e nome ho le quotazioni, seconda groupBy per ogni quotazione ho le aziende che hanno avuto stesso trend
		 JavaPairRDD<Iterable<String>, Iterable<String>> quotazioni = risultato.mapToPair(x -> new Tuple2<>(new Tuple2<>(x[0],x[1]), x[3])).groupByKey().filter(x -> ((Collection<String>)x._2()).size()==3 ).mapToPair(couple->new Tuple2<>(couple._2(), couple._1()._2()+";"+couple._1()._1())).groupByKey().filter(x-> ((Collection<String>)x._2()).size()>1).coalesce(1);                   
		
				
		quotazioni.saveAsTextFile(args[2]);
		
	}
	
	  private static String processString (String text)	{
		  
		    String[] fields= text.split(",");
			String ticker= fields[TICKER];
			String sector= fields[fields.length-2];
			String industry=fields[fields.length-1];
			String result = null;
			String exchange = fields[EXCHANGE];
			String name= fields[NAME];
			
			if (industry.equals("N/A") || exchange.equals("N/A") || ticker.equals("N/A") || sector.equals("N/A")|| name.equals("N/A")) {

				return result;
			}

			
			
			
	        if(industry.indexOf('"')!=-1) {
				
				sector= fields[fields.length-3];
			}
			
			
			
					
			if(name.indexOf('"')!=-1) {
				fields[2]=fields[2].replace('"', ' ');
				fields[3]=fields[3].replace('"', ' ');
				name=fields[2]+fields[3];
		        
			}
			
			
					
				 return result= ticker + "," + name + "," + sector ;
				 	
	  }
	

	private static Double transformDate(String dataToTrasform) {
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
