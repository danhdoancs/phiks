import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Phiks {

	String dataFile;
	String featureFile;

	Phiks(String dataFile, String featureFile) {
		String dataPath = "phiks/in/";
		this.dataFile = dataPath + dataFile;
		this.featureFile = dataPath + featureFile;
	}

	public void  run (int k) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "PHIKS");
		job1.setJarByClass(Phiks.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setCombinerClass(Job1Reducer.class);
		job1.setReducerClass(Job1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(dataFile));
		FileOutputFormat.setOutputPath(job1, new Path(featureFile));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

	class Job1Mapper
		extends Mapper<Object, Text, Text, IntWritable>{

			private final IntWritable one = new IntWritable(1);
			private Text word = new Text();

			public void map(Object key, Text value, Context context
					) throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString());
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					context.write(word, one);
				}
			}
		}

	class Job1Reducer
		extends Reducer<Text,IntWritable,Text,IntWritable> {
			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<IntWritable> values,
					Context context
					) throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);
				context.write(key, result);
			}
		}
}
