package MapReduce.MeanTemperatureCombiner;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Combinerclass extends Reducer<Text, Averagecountwritable, Text, Averagecountwritable> {

	@Override
	public void reduce(Text key, Iterable<Averagecountwritable> values, Context context)
			throws IOException, InterruptedException {
		String type;

		// Local variables to store the running sum and count of min temperatures for a station.
		Double sum_min_temp = 0.0;
		int sum_min_count = 0;

		// Local variables to store the running sum and count of max temperatures for a station.
		Double sum_max_temp = 0.0;
		int sum_max_count = 0;

		// Accumulate values based on the type of the temperature based on the station Id (Key)
		// Accumulate the running sum of temperatures and the count. Do not calucalte mean here.
		for (Averagecountwritable val : values) {
			type = val.temperature_type.toString();
			if (type.equals("TMAX")) {
				sum_max_temp += val.temperature.get();
				sum_max_count++;
			}
			if (type.equals("TMIN")) {
				sum_min_temp += val.temperature.get();
				sum_min_count++;
			}
		}
		

		// Write values to the reducer based on the type of temperature emitted from the reducer. 
		// The output to the reducer should be same as the input to the combiner or in other words output of the mapper. 
		context.write(key, new Averagecountwritable(new DoubleWritable(sum_max_temp), new IntWritable(sum_max_count), new Text("TMAX")));
		context.write(key, new Averagecountwritable(new DoubleWritable(sum_min_temp), new IntWritable(sum_min_count), new Text("TMIN")));

	}
}
