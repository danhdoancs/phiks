import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class PhiksJob1 {

	public static void main(String[] args) {
		String dataset = "/Users/daviddoan/Google Drive/OU/04_Job/dataset01/";
		SparkConf conf = new SparkConf().setAppName("Spark Civil Engineering Project");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

	//	JavaRDD<String> training = sc.textFile(dataset).cache();
	//	System.out.println(trainingset.sample(false,0.1).toString());
			

		//DataFrame training = sqlContext.read().format("arff")	.load("/Users/daviddoan/Google Drive/OU/04_Job/dataset01/civil_train_without_time.arff");
//		training.show();
	}


	public void map() {
	}

	public void reduce() {
	}
}
