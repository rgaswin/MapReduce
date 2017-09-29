package MapReduce.MeanTemperatureCombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//The Main Driver program to kickstart the map reduce reduce program with a combiner. 

public class MainClassCombiner {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Not enough parameters");
			System.exit(2);
		}
	
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MainClassCombiner.class);
		// Set the Mapper Class here
		job.setMapperClass(MapperclassCombiner.class);
		// Set the Combiner Class here
		job.setCombinerClass(Combinerclass.class);
		// Set the Reducer Class here
		job.setReducerClass(Reducerclass.class);
		// Set the Mapper Output key class here 
		job.setMapOutputKeyClass(Text.class);
		// Set the Mapper Output value class here 
		job.setMapOutputValueClass(Averagecountwritable.class);
		// Set the Reducer Output key class here 
		job.setOutputKeyClass(Text.class);
		// Set the Reducer Output Value class here 
		job.setOutputValueClass(Text.class);
		// Read Input path from the arguments 
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		// Set Output path from the arguments 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		//Exit after job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
