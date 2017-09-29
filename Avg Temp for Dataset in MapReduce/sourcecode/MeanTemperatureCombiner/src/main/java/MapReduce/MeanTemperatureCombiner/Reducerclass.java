package MapReduce.MeanTemperatureCombiner;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer class for the program. It inherits from the default Reducer class from the framework. 
//Input Key Type to the Reducer : Text
//Input Key Value to the Reducer : Averagecountwritable
//Output Key Type to the Reducer : Text
//Output Key Value to the Reducer : Text

public class Reducerclass extends Reducer<Text, Averagecountwritable, Text, Text> {

	// Overrides the default reduce method. 
	@Override
	public void reduce(Text key, Iterable<Averagecountwritable> values, Context context)
			throws IOException, InterruptedException {
		String type;

		Double mean_min_temp = 0.0;
		Double sum_min_temp = 0.0;
		int sum_min_count = 0;

		Double mean_max_temp = 0.0;
		Double sum_max_temp = 0.0;
		int sum_max_count = 0;

		// Accumulate all values belonging to the same key
		for (Averagecountwritable val : values) {
			type = val.temperature_type.toString();
			if (type.equals("TMAX")) {
				sum_max_temp += val.temperature.get();
				sum_max_count+= val.count.get();
			}
			if (type.equals("TMIN")) {
				sum_min_temp += val.temperature.get();
				sum_min_count+= val.count.get();
			}
		}
		// Calculate the mean minumum temperature for the particular key (station).
		if (sum_min_count != 0)
		{
			mean_min_temp = sum_min_temp / sum_min_count;
		}

		// Calculate the mean maximum temperature for the particular key (station).
		if (sum_max_count != 0) {
			mean_max_temp = sum_max_temp / sum_max_count;
		}

		// Concatenate the output with the min and max mean temperature.
		String final_output =  mean_min_temp.toString() + "," + mean_max_temp.toString();

		// Write the output to the output file
		context.write(key, new Text(final_output));

	}
}

