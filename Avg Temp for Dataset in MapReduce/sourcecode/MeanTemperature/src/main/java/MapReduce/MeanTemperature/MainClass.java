package MapReduce.MeanTemperature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


// The Main Driver program to kickstart the map reduce reduce program. 

public class MainClass {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Not enough parameters");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "simplemean");
		job.setJarByClass(MainClass.class);
		// Set the Mapper Class here
		job.setMapperClass(Mapperclass.class);
		// Set the Reducer Class here 
		job.setReducerClass(Reducerclass.class);
		// Set the Mapper Output key class here 
		job.setMapOutputKeyClass(Text.class);		
		// Set the Mapper Output value class here 
		job.setMapOutputValueClass(Averagecountwritable.class);	
		// Set the Reducer output key class
		job.setOutputKeyClass(Text.class);
		// Set the Reducer output value class
		job.setOutputValueClass(Text.class);
		// Read Input path from the arguments 
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		// Set output path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		// Exit job after completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}