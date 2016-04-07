import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.*;
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
import scala.Tuple2;

public class Phiks {
		/// Current MIKI
		static List<String> currentMiki = new ArrayList<>();
		static Map<List<String>, Integer> mikiMap = new HashMap<>();
		
		public static void main(String[] args) {
		//String dataset = "hdfs://localhost:9000/user/ddoan/phiks/input/cleaned_wiki_articles.txt";
		//String featureListFile = "hdfs://localhost:9000/user/ddoan/phiks/input/feature_list.txt";
		String dataset = "hdfs://localhost:9000/user/ddoan/phiks/input/debug_dataset.txt";
		String featureListFile = "hdfs://localhost:9000/user/ddoan/phiks/input/debug_features.txt";
		SparkConf conf = new SparkConf().setAppName("PHIKS");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		// Load training data from HDFS
		JavaRDD<List<String>> training = sc.textFile(dataset)
			.map(new Function<String, List<String>>() {
			public List<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});
		// Load feature list from HDFS
		JavaRDD<String> featureList = sc.textFile(featureListFile).cache();
		System.out.println("@@@@@@@@@@@@@@@ Output ##################");
		System.out.println("Training Input size: " + training.count());
		System.out.println("Training total partitions: " + training.getNumPartitions());
		System.out.println("Feature List size: " + featureList.count());
		System.out.println("Training data: " + training.first().get(0));
		
		// Init miki map	
		mikiMap.put(new ArrayList<String>(), 0);

		// Size itemset
		//int t = 1;
		// Size k
		int k = 4;
		// Size features
		long N = featureList.count();
		// Loop each step t = 1 to k: itemset size
		// Calculate projections
		// Calculate entropy of all candidates
		// Work on each partition
		/*JavaPairRDD<String, Integer> projPairs = training.mapPartitions(new Function<List<String>,Tuple2<List<String>, Integer>>() {
			public Tuple2<List<String>,Integer> call(List<String> s) {
				return new Tuple2(s,1);	
			}
		});
		*/
		training.mapToPair(s -> new Tuple2(s,1));

		for (int t = 1; t <= k; t++) {
			// Remaining feature list
			JavaRDD<String> remainingFeatures = featureList.filter(new Function<String, Boolean>() {
				public Boolean call(String s) {
					return !currentMiki.contains(s);
				}
			}); 

			//System.out.println("Remaining feature size: " + remainingFeatures.count());
			// Create hash map for each remaining feature
			Map<String,Map<List<String>, Integer>> maps = new HashMap<>();
			remainingFeatures.foreach(new VoidFunction<String>() {
				public void call(String s) {
					Map<List<String>,Integer> map = new HashMap<>();
					// Generate candidate's projections
					for (List<String> proj : mikiMap.keySet()) {
						// Add current miki projection with end .0 by item s into hash map of s
						map.put(proj, 0);	
						// Add current miki projection with end .1 by item s
						List<String> newProj = new ArrayList<>(proj);
						newProj.add(s);
						map.put(newProj, 0);
					}
					//System.out.println("Candidate: " + newProj.toString());
					System.out.println("Map: " + map.toString());
					maps.put(s, map);
					System.out.println("Maps: " + maps.toString());
				}
			});
			System.out.println("Maps size: " + maps.size());
			System.out.println("Maps: " + maps.toString());

			// Scan the data set to calcualte projection
			List<String> remainFeatureList = remainingFeatures.collect();
			System.out.println("Remain: " + remainFeatureList.toString());
			training.foreach(new VoidFunction<List<String>>() {
				public void call(List<String> T) {
					// Find S = transaction Intersect remainingFeatures
					List<String> S = intersection(T, remainFeatureList); 
					System.out.println("T: " + T.toString());
					System.out.println("S: " + S.toString());
					// Find projection of X on T
					List<String> mikiProj = intersection(T, currentMiki);
					// Increase frequency of projections
					for (String item : S) {
						List<String> key = new ArrayList<>(mikiProj);
						key.add(item);
						System.out.println("key: " + key.toString());
						System.out.println(maps.get(item).toString());
					}
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

	public static  <T> List<T> union(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<T>();

        set.addAll(list1);
        set.addAll(list2);

        return new ArrayList<T>(set);
    }

    public static List<String> intersection(List<String> list1, List<String> list2) {
        List<String> list = new ArrayList<String>();

        for (String t : list1) {
           if(list2.contains(t)) {
        	  list.add(t);
            }
        }

        return list;
    }
}
