package MapReduce.MeanTemperatureSecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MainClassSecondarySort {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Not enough parameters");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Secondary sort");
		job.setJarByClass(MainClassSecondarySort.class);
		// Set Mapper Class
		job.setMapperClass(MapperSecondaySort.class);
		// Set Partitioner Class
		job.setPartitionerClass(Partitionerclass.class);
		// Set Key comparator class
		job.setSortComparatorClass(keycomparator.class);
		// Set Grouping comparator class
		job.setGroupingComparatorClass(Groupcomparator.class);
		// Set Reducer class
		job.setReducerClass(ReducerSecondarySort.class);
 		// Set Map output key class type
		job.setMapOutputKeyClass(stringpair.class);
		// Set Map output value class type.
		job.setMapOutputValueClass(AveragecountwritableSecondary.class);
		// Set Reducer output class type
		job.setOutputKeyClass(Text.class);
		// Set Reduer output value class type
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
