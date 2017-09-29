package MapReduce.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankCalculationConfig {

	public static Double runjob(String input, String output, long num_of_nodes, double delta)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
		// Set Number of nodes value from the previous job.
		conf1.setLong("NUM_OF_NODES", num_of_nodes);
		// Set the delta value of the previous iteration.
		conf1.setDouble("PREVIOUS_DELTA", delta);
		Job job = Job.getInstance(conf1, "calcpagerank");

		job.setJarByClass(Driver.class);

		// Set the Mapper Class here
		job.setMapperClass(MapperPageRank.class);
		// Set the Reducer Class here
		job.setReducerClass(ReducerPageRank.class);
		// Set the Mapper Output key class here
		job.setMapOutputKeyClass(Text.class);
		// Set the Mapper Output value class here
		job.setMapOutputValueClass(Node.class);
		// Set the Reducer output key class
		job.setOutputKeyClass(Text.class);
		// Set the Reducer output value class
		job.setOutputValueClass(Text.class);
		// Read Input path from the arguments
		FileInputFormat.setInputPaths(job, new Path(input));
		// Set output path
		FileOutputFormat.setOutputPath(job, new Path(output));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		job.waitForCompletion(true);
	
		// Get the value of delta accumulated in the current iteration and send it back to the 
		// driver, so that it can be used in the next iteration.
		Double current_delta = job.getCounters().findCounter(Driver.CustomCounter.CURRENT_DELTA).getValue() / Math.pow(10,10);
	
		return current_delta;
	}
}
