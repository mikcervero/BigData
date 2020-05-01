package BigData.Job1;

import java.util.regex.Pattern;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class JobOneSpark {
	
	private static final Pattern COMMA = Pattern.compile(",");
	private static final Pattern DASH = Pattern.compile("-");

	public static void main(String[] args) {
		
		String file = "/home/fabiano/data/dataset.csv";
		
		SparkSession spark = SparkSession
				.builder()
				.appName("JobOne")
				.getOrCreate();
				
		
		JavaRDD<String> line = spark.read().textFile(file).javaRDD();
		
		JavaRDD<String[]> words = line.map(s -> s.split(",")).filter(x -> x.length==8);
	
		JavaRDD<String[]> filtro = words.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2008 && (Integer.parseInt(DASH.split(x[7])[0])) <=2018));
	
		
		JavaPairRDD<String, Double[]> tupla = filtro.mapToPair(x -> new Tuple2<>(x[0],new Double[] {Double.parseDouble(x[4]), Double.parseDouble(x[5]), Double.parseDouble(x[2]), transformDate(x[7]), Double.parseDouble(x[6]), 1.0}));

		JavaPairRDD<String, Double[]> agg = tupla.reduceByKey((x,y)-> new Double[] {Math.min(x[0],y[0]), Math.max(x[1], y[1]), chiusurainiziale(x[2],y[2],x[3],y[3]), chiusurafinale(x[2],y[2],x[3],y[3]), x[4]+y[4], x[5]+y[5]});

		JavaRDD<String> risultato = agg.map(couple -> String.valueOf(couple._1())+":"+String.valueOf(Math.round((couple._2()[3]/couple._2()[2])*100-100)+","+String.valueOf(couple._2()[0])+","+String.valueOf(couple._2()[1])+","+String.valueOf((couple._2()[4])/(couple._2()[5]))));

		risultato.saveAsTextFile("/home/fabiano/sparkresult.txt");
		
	
	
	}
	
	public static String stampa (Double[] a) {
		String b = "";
		for (int i=0; i<a.length; i++) {
			if (i==a.length-1) {
				b = b+(String.valueOf(a[i]));
				System.out.println(i);
			}
			else {
				b = b+(String.valueOf(a[i]))+",";
				System.out.println(i);
			}
		}
		return b;
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
	
	public static Double transformDate(String dataToTrasform ) {
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
