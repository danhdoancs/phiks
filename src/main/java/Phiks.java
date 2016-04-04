import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import java.util.Arrays;
import java.util.List;

public class Phiks {
	public static void main(String[] args) {
		String dataset = "hdfs://localhost:9000/user/ddoan/phiks/input/binary_wiki_articles.txt";
		String featureListFile = "hdfs://localhost:9000/user/ddoan/phiks/input/feature_list.txt";
		SparkConf conf = new SparkConf().setAppName("PHIKS");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> training = sc.textFile(dataset);
		JavaRDD<String> featureList = sc.textFile(featureListFile).cache();
		System.out.println("@@@@@@@@@@@@@@@ Output ##################");
		System.out.println("Training Input size: " + training.count());
		System.out.println("Feature List size: " + featureList.count());
		//System.out.println(training.first().toString());
		
		// Put data into array 
		JavaRDD<List<String>> trainArr = training.map(new Function<String, List<String>>() {
			@Override
			public List<String> call(String s) {
				return Arrays.asList(s.split(""));
			}
		});

		System.out.println("Train Arr: " + trainArr.count());
		//System.out.println(trainArr.first().toString());
		
		//
	}

	public void job1() {
	
	}

	public void job1_mapper() {
			
	}

	public void job1_reducer() {

	}

	public void job2() {

	}
}
