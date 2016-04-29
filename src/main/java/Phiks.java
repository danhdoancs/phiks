import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.AbstractMap;
import scala.Tuple2;

import com.google.common.collect.Lists;

public class Phiks implements Serializable {

	JavaRDD<List<String>> data;
	List<String> featureList;
	int N;
	long Fsize;	
	int k;

	Phiks(String dataFile, String featureFile, int k) {
		String dataPath = "file:///home/ddoan/Projects/java/phiks/datasets/";
		//String dataset = "hdfs://doan1.cs.ou.edu:9000/user/hduser/phiks/in/" + dataFile;
		String dataset = dataPath + dataFile;
		//String featureListFile = "hdfs://doan1.cs.ou.edu:9000/user/hduser/phiks/in/" + featureFile;
		String featureListFile = dataPath + featureFile;

		SparkConf conf = new SparkConf().setAppName("PHIKS");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// Load training data from HDFS
		Function<String, List<String>> spliter = new Function<String, List<String>>() {
			public List<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		};
		data = sc.textFile(dataset).map(spliter).cache();
		N = (int)data.count();
		System.out.println("Training data: " + N);
		System.out.println("Training total partitions: " + data.getNumPartitions());

		// Load feature list from HDFS
		//System.out.println(featureListFile);
		featureList = new ArrayList<>(sc.textFile(featureListFile).toArray());
		// Size features
		Fsize = featureList.size();
		System.out.println("Feature list size: " + Fsize);
		// Init k
		this.k = k;
	}

	List<String> run() {
		// Check k size
		if (k < 1 || k >= Fsize) {
			System.err.println("K value is invalid. 0 < k < featureSize");
			return null;
		}

		//Broadcast<List<String>> bcFeatureList = sc.broadcast(featureList);
		System.out.println("@@@@@@@@@@@@@@@ Output ##################");

		runJob1();
		return null;
	}

	void runJob1() {
		// Work on each partition
		JavaRDD<String> projections = data.mapPartitions(new FlatMapFunction<Iterator<List<String>>, String>() {
				public Iterable<String> call(Iterator<List<String>> tranIt) {
				// Cache partition into memory as ArrayList
				List<List<String>> subset = Lists.newArrayList(tranIt);
				System.out.println(": Subset size: " + subset.size());
				// Start timer
				long startTime = System.nanoTime();
				// Init 2 core variables to keep track of local miki
				// Current MIKI
				List<String> localMiki = new ArrayList<>();
				// Init miki map	
				Map<List<String>,Integer> mikiMap = new LinkedHashMap<>();
				mikiMap.put(new ArrayList<String>(), 0);
				// Loop k times to find local MIKI
				for (int t=1; t<=k; t++) {
				// Get remain features;
				List<String> remainFeatures = getRemainFeatures(localMiki, featureList);
				//System.out.println(t+": Remain features: " + remainFeatures);
				// Get candidate set
				List<List<String>> candidates = getCandidates(remainFeatures, localMiki);	
				//System.out.println(t+": Candidates: " + candidates.toString());
				// Generate feature maps
				Map<String,Map<List<String>,Integer>> featureMaps = generateFeatureMaps(remainFeatures, mikiMap);
				//System.out.println(t+": Feature maps: " + featureMaps.toString());
				// Scan the data split
				// For each transaction T, get S = T intesect F/X
				for (List<String> tran : subset) {
					// Get S = T intersect F-X 	
					// Find S = transaction Intersect remainingFeatures
					List<String> S = intersection(tran, remainFeatures); 
					//System.out.println(": Transaction: " + tran.toString());
					//System.out.println(": S " + S.toString());
					// Find projection of current miki on T
					List<String> mikiProj = intersection(tran, localMiki);
					//System.out.println(": Miki proj: " + mikiProj.toString());
					// Increase frequency of projections
					List<Tuple2<Map.Entry<String,List<String>>,Integer>> result = new ArrayList<>();
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

				// Update miki
				// Get freqency of feature.0 projections end 0
				// p.0 = p - p.1
				updateFeatureMaps(featureMaps, mikiMap, N);
				//System.out.println(t+": feature maps: " + featureMaps.toString());
				//System.out.println("before miki: " + localMiki.toString());
				mikiMap = updateCurrentMiki(featureMaps, localMiki, N);
				// End timer
				long elapsedTime = System.nanoTime() - startTime;
				double elapsedSeconds = (double)elapsedTime / 1000000000.0;
				System.out.println(t+":@@@@@@@ Current miki: " + localMiki.toString());
				System.out.println(t+":@@@@@@@ Elapsed Time: " + elapsedSeconds + " seconds.");
				}
				return localMiki;
				}
		});

		System.out.println(data.collect().toString());
	}

	Map<List<String>,Integer> updateCurrentMiki(Map<String,Map<List<String>,Integer>> featureMaps, List<String> localMiki, int splitSize) {
		if (featureMaps == null) {
			System.err.println("ERROR: feature maps is null");
			return null;
		}
		if (localMiki == null) {
			System.err.println("ERROR: current miki is null");
			return null;
		}

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
			//System.out.println(feature + ": entropy = " + entropy);
		}
		// Update current miki
		localMiki.add(candidate);
		return featureMaps.get(candidate); 
	}

	double computeJointEntropy(String candidate,Map<String,Map<List<String>,Integer>> featureMaps, int splitSize) {
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

	void updateFeatureMaps(Map<String,Map<List<String>,Integer>> featureMaps, Map<List<String>,Integer> mikiMap, int splitSize) {
		if (featureMaps == null) {
			System.err.println("ERROR: feature maps is null");
			return; 
		}
		if (mikiMap == null) {
			System.err.println("ERROR: current miki is null");
			return;
		}

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
				// Check if projKey0 is valid or not
				// If not, skip
				if(featureMap.containsKey(projKey0) == false) {
					continue;
				}

				// Value for p.0
				//System.out.println("1="+projEntry.getValue()+", 2=" +  (int) mikiMap.get(projKey0) + ", 3=" + splitSize + ", 4=" + mikiMap.size());
				int projValue0 = (int) projEntry.getValue();
				if (mikiMap.size() > 1 && mikiMap.get(projKey0) != null) {
					//System.out.println("projKey0: " + projKey0);
					//System.out.println("projValue0: " + (int) mikiMap.get(projKey0));
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

	Map<String,Map<List<String>,Integer>> generateFeatureMaps(List<String> remainFeatures, Map<List<String>,Integer> mikiMap) {
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

	List<String> getRemainFeatures(List<String> localMiki, List<String> featureList) {
		// Get remain features 
		List<String> remainFeatures = new ArrayList<>();
		for (String feature : featureList) {
			if (!localMiki.contains(feature)) {
				remainFeatures.add(feature);	
			}
		}
		return remainFeatures;
	}

	List<List<String>> getCandidates(List<String> remainFeatures, List<String> localMiki) {
		// Combine each remain feature with current miki to create new candidate
		List<List<String>> candidates = new ArrayList<>();
		for (String feature : remainFeatures) {
			List<String> candidate = new ArrayList<>(localMiki);
			candidate.add(feature);
			candidates.add(candidate);
		}
		return candidates;
	}

	List<String> intersection(List<String> list1, List<String> list2) {
		List<String> list = new ArrayList<String>();

		for (String t : list1) {
			if(list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}
}
