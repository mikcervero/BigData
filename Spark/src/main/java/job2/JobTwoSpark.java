package job2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import Parser.Parser;
import scala.Tuple2;
import scala.Tuple3;

public class JobTwoSpark {

	private static final Pattern COMMA = Pattern.compile(",");
	private static final Pattern DASH = Pattern.compile("-");
	private static final int TICKER = 0;
	private static final int NAME = 2;

	public static void main(String[] args) {

		String file1 = "/home/fabiano/data/historical_stocks.csv";
		String file2 = "/home/fabiano/data/historical_stock_prices.csv";

		SparkSession spark = SparkSession.builder().appName("JobTwo").getOrCreate();

		JavaRDD<String> line1 = spark.read().textFile(file1).javaRDD();
		JavaRDD<String> line2 = spark.read().textFile(file2).javaRDD();

		JavaRDD<String[]> words1 = line1.map(s -> processString(s)).filter(x -> x != null)
				.map(x -> new String[] { x.split(",")[0], x.split(",")[2] });
		JavaRDD<String[]> words2 = line2.map(s -> s.split(",")).filter(x -> x.length == 8);

		JavaRDD<String[]> filtro2 = words2.filter(x -> ((Integer.parseInt(DASH.split(x[7])[0])) >= 2008
				&& (Integer.parseInt(DASH.split(x[7])[0])) <= 2018));

		JavaPairRDD<String, String[]> stocks = words1.mapToPair(x -> new Tuple2<>(x[0], new String[] { x[1] }));
		JavaPairRDD<String, String[]> prices = filtro2
				.mapToPair(x -> new Tuple2<>(x[0], new String[] { x[2], x[6], x[7], DASH.split(x[7])[0] }));

		JavaPairRDD<String, Tuple2<String[], String[]>> join = stocks.join(prices);

		// ticker,settore, chiusura, volume, data, anno
		JavaRDD<String[]> joinresult = join.map(couple -> new String[] { couple._1(), couple._2()._1[0], couple._2()._2[0], couple._2()._2[1], couple._2()._2[2], couple._2()._2[3] });

		//((ticker,settore,anno)          ->   volume, chiusurainiziale, chiusurafinale, datainiziale, datafinale ,1(conteggio record),chiusura)
		JavaPairRDD<Tuple3<String,String,String>, Double[]> a = joinresult.mapToPair(x -> new Tuple2<>(new Tuple3<>(x[0],x[1],x[5]), new Double[] { Double.parseDouble(x[3]), Double.parseDouble(x[2]), Double.parseDouble(x[2]), transformDate(x[4]), transformDate(x[4]), 1.0, Double.parseDouble(x[2])}));
		
		//((ticker,settore,anno)      -> somma_volume, chiusurainiziale, chisurafinale, datainiziale, datafinale, conteggiorecord, somma_chiusure)
		JavaPairRDD<Tuple3<String,String,String>, Double[]> b = a.reduceByKey((x, y) ->  {
			Double datainiziale ;
			Double chiusurainiziale;
			Double datafinale;
			Double chiusurafinale;
			if (x[3]<y[3]) {
				datainiziale = x[3];
				chiusurainiziale = x[1];
			}
			else {
				datainiziale = y[3];
				chiusurainiziale = y[1];
			}
			if (x[4]>y[4]) {
				datafinale = x[4];
				chiusurafinale = x[2];
			}
			else {
				datafinale = y[5];
				chiusurafinale = y[2];
			}		
			
			return new Double[] {x[0] + y[0], chiusurainiziale, chiusurafinale, datainiziale, datafinale, x[5] + y[5], x[6]+y[6] };
		});
		
		//(ticker,settore,anno,somma_volume,quotazione,quotazione_giornaliera)
		JavaRDD<String[]> c = b.map(x -> new String[] {x._1()._1(),x._1()._2(),x._1()._3(),String.valueOf(x._2()[0]),String.valueOf(Math.round((x._2()[2]/x._2()[1])*100-100)),String.valueOf(x._2()[6]/x._2()[5])});
		
		//((settore,anno)         ->       volume,1, quotazione,quotazione_giornaliera)
		JavaPairRDD<Tuple2<String,String>, Double[]> intermedio = c.mapToPair(x -> new Tuple2<>(new Tuple2<>(x[1],x[2]), new Double[] { Double.parseDouble(x[3]), 1.0, Double.parseDouble(x[4]), Double.parseDouble(x[5]) }));
		
		//((settore,anno)         ->       somma_volume, conteggio record, somma_quotazioni_annuali, somma_quotazioni_giornaliere)
		JavaPairRDD<Tuple2<String,String>, Double[]> agg = intermedio.reduceByKey((x, y) -> new Double[] { x[0] + y[0], x[1] + y[1], x[2] +y[2], x[3]+y[3] });

		JavaRDD<String> risultato = agg.map(x -> x._1()._1()+","+x._1()._2() + "," + String.valueOf(x._2()[0] / x._2()[1])+ ","+ String.valueOf(x._2()[2]/x._2()[1])+","+String.valueOf(x._2()[3]/x._2()[1])).coalesce(1);

		risultato.saveAsTextFile("/home/fabiano/risultato.txt");

	}

	private static String processString(String text) {

		String[] fields = text.split(",");
		String ticker = fields[TICKER];
		String sector = fields[fields.length - 2];
		String industry = fields[fields.length - 1];
		String result = null;

		if (industry.indexOf('"') != -1) {

			sector = fields[fields.length - 3];
		}

		if (sector.equals("N/A")) {

			return result;
		}

		if (fields[NAME].indexOf('"') != -1) {
			fields[NAME] = fields[2].replace('"', ' ');
			fields[3] = fields[3].replace('"', ' ');
			fields[NAME] = fields[2] + fields[3];

		}

		return result = ticker + "," + fields[NAME] + "," + sector;

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
