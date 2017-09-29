package MapReduce.MeanTemperature;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper class for the program. It inherits from the default Mapper class from the framework. 
// Input Key Type to the mapper : Object
// Input Key Value to the mapper : Object
// Output Key Type to the mapper : Text
// Output Key Value to the mapper : Averagecountwritable
public class Mapperclass extends Mapper<Object, Text, Text, Averagecountwritable> {

	// Create a new IntWritable object with a value 1
	// Source : Word count program

	// Overrides the map and implements its functionality.
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String string_value = value.toString();
		String[] read_data = string_value.split(",");
		String temp_type = read_data[2];
		String station = read_data[0];
		String temperature_str = read_data[3];
		Double temperature;
		// Try to convert string to double or default to 0.0
		try {
			temperature = Double.parseDouble(temperature_str);
		} catch (Exception e) {
			temperature = 0.0;
		}

		// Write the output to the context
		context.write(new Text(station),
				new Averagecountwritable(new DoubleWritable(temperature), new IntWritable(1), new Text(temp_type)));
	}
}
