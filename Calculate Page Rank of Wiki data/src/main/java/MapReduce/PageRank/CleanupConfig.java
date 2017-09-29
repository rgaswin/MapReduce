package MapReduce.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CleanupConfig {

	public static long runjob(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "cleanupJob");		
		job.setJarByClass(Driver.class);
		// Set the Mapper Class here
		job.setMapperClass(CleanupMapper.class);
		// Set the Reducer Class here
		job.setReducerClass(CleanupReducer.class);
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
		FileOutputFormat.setOutputPath(job,new Path(output));
		job.waitForCompletion(true);		
		// Get the number of nodes from the global counter after the job is completed.
		long num_of_nodes = job.getCounters().findCounter(Driver.CustomCounter.NUM_OF_NODES).getValue();		
		// Return the number of nodes to the driver program.
		return num_of_nodes;
	}
}
