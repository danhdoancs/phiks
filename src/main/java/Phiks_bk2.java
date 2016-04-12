import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.broadcast.Broadcast;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Iterator;
import scala.Tuple2;

public class Phiks_bk2 {
	static List<String> featureList; 

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
		featureList = new ArrayList<>(sc.textFile(featureListFile).toArray());
		Broadcast<List<String>> bcFeatureList = sc.broadcast(featureList);
		System.out.println("@@@@@@@@@@@@@@@ Output ##################");
		//System.out.println("Training Input size: " + training.count());
		//System.out.println("Training total partitions: " + training.getNumPartitions());
		//System.out.println("Feature List size: " + featureList.size());
		//System.out.println("Training data: " + training.first().get(0));

		/// Current MIKI
		List<String> currentMiki;
		Map<List<String>,Integer> mikiMap;
		currentMiki = new ArrayList<>();
		// Init miki map	
		mikiMap = new LinkedHashMap<>();
		mikiMap.put(new ArrayList<String>(), 0);

		// Size itemset
		//int splitSize = 0;
		//int t = 1;
		// Size k
		int k = 3;
		// Size features
		long Fsize = featureList.size();

		
		// Loop each step t = 1 to k: itemset size
		// Calculate projections
		// Calculate entropy of all candidates
		// Work on each partition
		// Calculate entropy for each 1-itemset
		// Using prefix/suffix to find projections
		// For each size of itemset
		for (int t = 1; t <= k; t++) {
			// Broadcast the size t from 1 - k
			Broadcast<Integer> bcItemSize = sc.broadcast(t);
			Broadcast<List<String>> bcCurrentMiki = sc.broadcast(currentMiki);
			Broadcast<Map<List<String>,Integer>> bcMikiMap = sc.broadcast(mikiMap);
			// Work on each partition
			training.foreachPartition(new VoidFunction<Iterator<List<String>>>() {
					public void call(Iterator<List<String>> tranIt) {
					// Get itemset size t
					int t = bcItemSize.value();
					List<String> currentMiki = bcCurrentMiki.value();
					Map<List<String>,Integer> mikiMap = bcMikiMap.value();
					List<String> featureList = bcFeatureList.value();
					System.out.println("t: " + t);
					System.out.println(" miki: " + currentMiki);
					System.out.println("miki map: " + mikiMap);
					System.out.println("feature list: " + featureList);
					// Get remain features;
					List<String> remainFeatures = getRemainFeatures(currentMiki, featureList);
					System.out.println(": Remain features: " + remainFeatures);
					// Get candidate set
					List<List<String>> candidates = getCandidates(remainFeatures, currentMiki);	
					System.out.println(": Candidates: " + candidates.toString());
					// Generate feature maps
					Map<String,Map<List<String>,Integer>> featureMaps = generateFeatureMaps(remainFeatures, mikiMap);
					// Scan the data split
					// For each transaction T, get S = T intesect F/X
					int splitSize = 0;
					while (tranIt.hasNext()) {
						splitSize++;
						List<String> tran = tranIt.next();
						// Get S = T intersect F-X 	
						// Find S = transaction Intersect remainingFeatures
						List<String> S = intersection(tran, remainFeatures); 
						System.out.println(": Transaction: " + tran.toString());
						System.out.println("S: " + S.toString());
						// Find projection of current miki on T
						List<String> mikiProj = intersection(tran, currentMiki);
						System.out.println("Miki proj: " + mikiProj.toString());
						// Increase frequency of projections
						for (String item : S) {
							// Create feature key 
							List<String> key = new ArrayList<>();
							key.add(item);
							// Retrieve the feature map
							Map<List<String>,Integer> featureMap = featureMaps.get(item);
							// Retrieve the projection pair of feature
							List<String> projKey = new ArrayList<>(mikiProj);
							projKey.add(item);
							// Increase frequency
							featureMap.put(projKey,featureMap.get(projKey) + 1);
							//System.out.println(featureMaps.get(item).toString());
						}
					}
					System.out.println("feature maps: " + featureMaps.toString());
					// Get freqency of feature.0 projections end 0
					// p.0 = p - p.1
					updateFeatureMaps(featureMaps, mikiMap, splitSize);
					System.out.println("feature maps: " + featureMaps.toString());
					System.out.println("before miki: " + currentMiki.toString());
					System.out.println("before miki map: " + mikiMap.toString());
					updateCurrentMiki(featureMaps, currentMiki, mikiMap, splitSize);
					System.out.println("current miki: " + currentMiki.toString());
					// Check if i != k
					// Compute entropy
					// Else return candidate sets
					}
				});
			}
		}

		public static void updateCurrentMiki(Map<String,Map<List<String>,Integer>> featureMaps, List<String> currentMiki, Map<List<String>,Integer> mikiMap, int splitSize) {
			double maxEntropy = 0.0;	
			String candidate = null;
			//System.out.println("miki feature maps: " + featureMaps.toString());
			for (Map.Entry entry : featureMaps.entrySet()) {
				Map<List<String>,Integer> featureMap = (Map<List<String>,Integer>) entry.getValue(); 
				String feature = (String) entry.getKey();
				double entropy = computeJointEntropy(feature,featureMaps, splitSize); 
				if (maxEntropy < entropy){
					maxEntropy = entropy;
					candidate = feature;
				}
				System.out.println(feature + ": entropy = " + entropy);
			}
			// Update current miki
			currentMiki.add(candidate);
			mikiMap = featureMaps.get(candidate); 
			//System.out.println("final miki: " + currentMiki.toString());
		}

		public static double computeJointEntropy(String candidate,Map<String,Map<List<String>,Integer>> featureMaps, int splitSize) {
			Map<List<String>,Integer> candProjs = featureMaps.get(candidate);
			double entropy = 0.0;
			//System.out.println("miki cand projs: " + candProjs.values().toString());
			//System.out.println("miki split size: " + splitSize);
			for (int i : candProjs.values()) {
				if (i > 0) {
					double prob = (double)i/splitSize;
					entropy -= prob*Math.log(prob);
				}
			}
			return entropy;
		}

		public static void updateFeatureMaps(Map<String,Map<List<String>,Integer>> featureMaps, Map<List<String>,Integer> mikiMap, int splitSize) {
			for (Map.Entry entry : featureMaps.entrySet()) {
				Map<List<String>,Integer> featureMap = (Map<List<String>,Integer>) entry.getValue(); 
				String feature = (String) entry.getKey();
				int idx = 0;
				// Loop throuh projection of current miki
				for (Map.Entry projEntry : featureMap.entrySet()) {
					// Only loop through p.1 proj
					if (idx++ % 2 != 0) 
						continue;
					// Keys for p.1
					List<String> projKey1 = (List<String>) projEntry.getKey(); 
					// Key for p.0
					List<String> projKey0 = new ArrayList<>(projKey1);
					projKey0.remove(feature);
					// Value for p.0
					//System.out.println("1="+projEntry.getValue()+", 2=" +  (int) mikiMap.get(projKey0) + ", 3=" + splitSize + ", 4=" + mikiMap.size());
					int projValue0 = (int) projEntry.getValue();
					if (mikiMap.size() > 1) {
						projValue0 = (int) mikiMap.get(projKey0) - projValue0; 
					} else {
						projValue0 = splitSize - projValue0;
					}
					// Update value for p.0
					featureMap.put(projKey0, projValue0);
					//System.out.println("Updated feature map: " + featureMap.toString());
				}
				// Update
				featureMaps.put(feature,featureMap);
			}
		}

		public static Map<String,Map<List<String>,Integer>> generateFeatureMaps(List<String> remainFeatures, Map<List<String>,Integer> mikiMap) {
			Map<String,Map<List<String>, Integer>> maps = new LinkedHashMap<>();
			for (String feature : remainFeatures) {
				Map<List<String>,Integer> map = new LinkedHashMap<>();
				// Generate candidate's projections
				for (List<String> proj : mikiMap.keySet()) {
					//System.out.println("miki proj: " + proj.toString());
					// Add current miki projection with end .1 by item s
					List<String> newProj = new ArrayList<>(proj);
					newProj.add(feature);
					map.put(newProj, 0);
					// Add current miki projection with end .0 by item s into hash map of s
					map.put(proj, 0);	
					//System.out.println("miki map: " + map.toString());
				}
				//System.out.println("Map: " + map.toString());
				maps.put(feature, map);
			}
			//System.out.println("Maps: " + maps.toString());
			return maps;
		}

		public static List<String> getRemainFeatures(List<String> currentMiki, List<String> featureList) {
			// Get remain features 
			List<String> remainFeatures = new ArrayList<>();
			for (String feature : featureList) {
				if (!currentMiki.contains(feature)) {
					remainFeatures.add(feature);	
				}
			}
			return remainFeatures;
		}

		public static List<List<String>> getCandidates(List<String> remainFeatures, List<String> currentMiki) {
			// Combine each remain feature with current miki to create new candidate
			List<List<String>> candidates = new ArrayList<>();
			for (String feature : remainFeatures) {
				List<String> candidate = new ArrayList<>(currentMiki);
				candidate.add(feature);
				candidates.add(candidate);
			}
			return candidates;
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
