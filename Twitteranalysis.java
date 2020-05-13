import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.*;


public class Twitteranalysis {

	public static void main(String args[]) throws Exception
	{
		//I have used Logger to switch off the info log of the spark 
		Logger.getLogger("org.apache").setLevel(Level.OFF);	
		//This part is the initialization of the spark context . I have given the app name as "CountTemperature" and set the master to 
		//Local as it os running locally.
		SparkConf sparkConf = new SparkConf().setAppName("TwitterAnalysis").setMaster("local[4]").set("spark.executor.memory",
				"1g");

		//To connect to my developer twitter account I have set up the keys in system property to connect
		System.setProperty("twitter4j.oauth.consumerKey", "*********************");
		System.setProperty("twitter4j.oauth.consumerSecret", "**************************");
		System.setProperty("twitter4j.oauth.accessToken", "*********");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "*********");
		
		//Here I am initiating the Javastreamingcotext saying that the context duration will be of 1 second
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		//Here I am initiating the a DStream which is representing a stream of tweet data in that window duration
		JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);

		//This part is the Dstream of String which will hold the text of every tweet Dstream and mapping it so that I can get the i
		//individual tweets text.
		JavaDStream<String> statuses = tweets.map(

				x ->x .getText()
				);

		// print first 10 records in each RDD in a DStream
		//If you want to print more than 10 tweets please specifiy the number to be printed
		statuses.print();

		//This is Dstream of type integers which holds the number of words per tweet. This is based on the space splitting and taking the 
		//size of the number of the data in the array where the words are stored
		JavaDStream<Integer> words1 = tweets.map(

				x -> Arrays.asList(x.getText().split(" ")).size());

		words1.print();
		
		//This is Dstream of type integers which holds the number of words per tweet. This is based on the space splitting and taking the 
		//size of the number of the data in the array where the words are stored
		JavaDStream<Integer> characters1 = tweets.map(

				x -> Arrays.asList(x.getText().split("")).size());

		characters1.print();

		 

		//This part is to get the hashtags. To do this we have checked the wors of the text of the tweets and checked the words which
		//has been started with '#'
		JavaDStream<String> wordsTotal = statuses.flatMap((String x) -> Arrays.asList(x.split(" ")).iterator());
		JavaDStream<String> hashtagsTotal = wordsTotal.filter(x -> x.startsWith("#"));
		hashtagsTotal.print();
		//This is the part which is getting the average number of words per tweet. We are getting by dividing the total number of words
		//and total number of tweets
		statuses.foreachRDD(f ->
		{
			if(f.count()>0)
			{
				JavaRDD<String> words5 = f.flatMap((String x) -> Arrays.asList(x.split(" ")).iterator());
				Long countstatus = f.count();
				//System.out.println(countstatus);
				System.out.println("Average Number of Words per tweet is : " +words5.count()/countstatus);
			}

		}
				);
		
		//This is the part which is getting the average number of chracters per tweet. We are getting by dividing the total number of words
		//and total number of tweets

		statuses.foreachRDD(f ->
		{
			if(f.count()>0)
			{
				JavaRDD<String> words6 = f.flatMap((String x) -> Arrays.asList(x.split("")).iterator());
				long countstatus = f.count();
				System.out.println("Average Number of characters per tweet is : " +words6.count()/countstatus);
			}

		}
				);



		//This is the part which gives the top 10 hastags in the tweets stream. We are first reducing the data with the number
		//counts. Then mapping as pair. Then it is getting sorted taking the number of counts as key.
		JavaPairDStream<String, Integer> ones = hashtagsTotal.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1));
		JavaPairDStream<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

		JavaPairDStream<Integer, String> swappedCounts = counts.map(f -> f.swap()).mapToPair(x -> new Tuple2<Integer, String>(x._1(),x._2()));
		JavaPairDStream<Integer, String> sortedCounts =swappedCounts.transformToPair(
				new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
					public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
						return in.sortByKey(false);
					}
				});



		sortedCounts.foreachRDD(f ->
		{

			if(f.count()>0)
			{
				JavaPairRDD<Integer, String> topList = f;
				String out = "\nTop 10 hashtags:\n";
				for (Tuple2<Integer, String> t : topList.take(10)) {
					out = out + t.toString() + "\n";
				}
				System.out.println(out);
			}
		});
		
		//This s the tweets Dstream of texts base don the window size .
		JavaDStream<String> statuseswithWindow = statuses.window(new Duration(60 * 5 * 1000), new Duration(30 * 1000));
		//This is the part which is getting the average number of words per tweet. We are getting by dividing the total number of words
		//and total number of tweets. Only thing is we have given the wins=dow size and time spam now according to question.
		statuseswithWindow.foreachRDD(f ->
		{
			if(f.count()>0)
			{
				JavaRDD<String> words5 = f.flatMap((String x) -> Arrays.asList(x.split(" ")).iterator());
				Long countstatus = f.count();
				System.out.println(countstatus);
				System.out.println("Average Number of Words per tweet is : " +words5.count()/countstatus);
			}

		}
				);
		
		//This is the part which is getting the average number of characters per tweet. We are getting by dividing the total number of words
		//and total number of tweets. Only thing is we have given the wins=dow size and time spam now according to question.

		statuseswithWindow.foreachRDD(f ->
		{
			if(f.count()>0)
			{
				JavaRDD<String> words6 = f.flatMap((String x) -> Arrays.asList(x.split("")).iterator());
				long countstatus = f.count();
				System.out.println("Average Number of characters per tweet is : " +words6.count()/countstatus);
			}

		}
				);
		
		

		//This is the part which gives the top 10 hastags in the tweets stream. We are first reducing the data with the number
		//counts. Then mapping as pair. Then it is getting sorted taking the number of counts as key.Only thing is we are setting the window
		//Value now as per the question 
		JavaPairDStream<String, Integer> countsWithWindow = ones.reduceByKeyAndWindow((Integer i1, Integer i2) -> i1 + i2
				, new Duration(60 * 5 * 1000),
				new Duration(30 * 1000));

		JavaPairDStream<Integer, String> swappedCountsWithWindow = countsWithWindow.map(f -> f.swap()).mapToPair(x -> new Tuple2<Integer, String>(x._1(),x._2()));
		JavaPairDStream<Integer, String> sortedCountsWithWindow =swappedCountsWithWindow.transformToPair(
				new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
					public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
						return in.sortByKey(false);
					}
				});


		sortedCountsWithWindow.foreachRDD(f ->

		{
			if(f.count()>0)
			{
				JavaPairRDD<Integer, String> topList = f;
				String out = "\nTop 10 hashtags for the window size mentioned:\n";
				for (Tuple2<Integer, String> t : topList.take(10)) {
					out = out + t.toString() + "\n";
				}
				System.out.println(out);
			}

		});





		ssc.start();
		ssc.awaitTermination();

	}


}

