import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

public class Phiks {
		/// Current MIKI
		static Set<String> currentMiki = new HashSet<>();
		
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
		System.out.println("Training total partitions: " + training.getNumPartitions());
		System.out.println("Feature List size: " + featureList.count());
		//System.out.println(training.first().toString());
		
		// Put data into array 
		JavaRDD<List<Boolean>> trainArr = training.map(new Function<String, List<Boolean>>() {
			public List<Boolean> call(String s) {
				return Arrays.asList(Arrays.asList(s.split(""))
				.stream()
				.map(item -> item.equals("1"))
				.toArray(Boolean[]::new));
			}
		});

		System.out.println("Train Arr: " + trainArr.count());
		//System.out.println(trainArr.first().toString());
		
		// Size itemset
		//int t = 1;
		// Size k
		int k = 4;
		// Size features
		long N = featureList.count();
		// Compute entropy for each feature in feature list
		for (int t = 1; t <= k; t++) {
			// Remaining feature list
			JavaRDD<String> remainingFeatures = featureList.filter(new Function<String, Boolean>() {
				public Boolean call(String s) {
					return !currentMiki.contains(s);
				}
			}); 

			System.out.println("Remaining feature size: " + remainingFeatures.count());
			// Create hash map for each remaining feature
			Set<Map<Set<String>, Integer>> maps = new HashSet<>();
			remainingFeatures.foreach(new VoidFunction<String>() {
				public void call(String s) {
					Set<String> candidate = new HashSet<>(currentMiki);
					candidate.add(s);
					Map<Set<String>,Integer> map = new HashMap<>();
					// Init all project of X + i
					map.put(candidate, 0);
					//System.out.println(map.toString());
				}
			});

			// Scan the data set to calcualte projection
			trainArr.foreach(new VoidFunction<List<Boolean>>() {
				public void call(List<Boolean> T) {
					// Find S = transaction Intersect remainingFeatures
					//List<Boolean> S =
				}
			});
		}

		/*
		featureList.map(new Function<String, String>() {
			public String call(String s) {
				
			}
		});
		*/
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
