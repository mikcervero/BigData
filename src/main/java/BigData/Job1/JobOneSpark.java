package BigData.Job1;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class JobOneSpark {
	
	private static final Pattern COMMA = Pattern.compile(",");
	private static final Pattern SPACE = Pattern.compile("-");
	private final int SYMBOL = 0;
	private final int PREZZOCHIUSURA = 2;
	private final int PREZZOMINIMO = 4;
	private final int PREZZOMASSIMO = 5;
	private final int VOLUME = 6;
	private final int DATE = 7; 
	
	public static void main(String[] args) {
		
		String file = "/home/fabiano/data/historical_stock_prices.csv";
		
		SparkSession spark = SparkSession
				.builder()
				.appName("JobOne")
				.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(file).javaRDD();
		
		JavaRDD<String[]> words = lines.map(s -> COMMA.split(s));
		//words.foreach(x -> System.out.println(Integer.parseInt(SPACE.split(x[7])[0])));
		//JavaRDD<Stirng[]> iltro = words.foreach(x -> );
		JavaRDD<String[]> filtro = words.filter(x -> ((Integer.parseInt(SPACE.split(x[7])[0])) >= 2008 && (Integer.parseInt(SPACE.split(x[7])[0])) <=2018));
		//((Integer.parseInt(x[DATE].split('-')[0]) >= 2008) && (Integer.parseInt(x[DATE].split('-')[0])<=2018)));
		System.out.println("lunghezza"+filtro.count());
		

		
	
	}
	
}
