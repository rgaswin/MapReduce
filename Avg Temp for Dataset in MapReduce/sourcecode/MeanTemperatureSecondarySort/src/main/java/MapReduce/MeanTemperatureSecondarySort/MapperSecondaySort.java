package MapReduce.MeanTemperatureSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSecondaySort extends Mapper<Object, Text, stringpair, AveragecountwritableSecondary> {

	// Implement the map function for the class. 
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// local variable declarations
		String string_value = value.toString();
		String[] read_data = string_value.split(",");
		String station = read_data[0];
		String year_str = read_data[1];
		// Get the year value from the year string. 
		int year = Getyear(year_str);
		String temp_type = read_data[2];
		String temperature_str = read_data[3];
		Double temperature;
		try {
			temperature = Double.parseDouble(temperature_str);
		} catch (Exception e) {
			temperature = 0.0;
		}
		
		// Write to the context with year and station as key 
		// (Year, temperature, count, temperature type) are the values to the mapper output 
		context.write(new stringpair(year, station), new AveragecountwritableSecondary(new IntWritable(year),
				new DoubleWritable(temperature), new IntWritable(1), new Text(temp_type)));
	}

	// A function to extract year from the date string from input
	public int Getyear(String date) {
		int value = 0;

		// Return the first four digits
		try {
			value = Integer.parseInt(date.substring(0, 4));
		} catch (Exception e) {
			value = 0;
		}
		return value;
	}

}
