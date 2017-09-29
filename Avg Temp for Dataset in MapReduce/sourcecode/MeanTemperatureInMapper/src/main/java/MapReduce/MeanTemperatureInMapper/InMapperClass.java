package MapReduce.MeanTemperatureInMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InMapperClass extends Mapper<Object, Text, Text, Averagecountwritable> {

	public HashMap<String, Averagecountwritable> station_hashmap;

	@Override	
	// Initialize a datastructure at the setup. 
	// This structure enables accumalation of data through the lifecycle of the Map task.
	public void setup(Context context) {
		station_hashmap = new HashMap<String, Averagecountwritable>();
	}

	@Override
	// Implement the map method
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Local variables declared here
		String string_value = value.toString();
		String[] read_data = string_value.split(",");
		String temp_type = read_data[2];
		String station = read_data[0];
		// Key for the hash map for accumulating data. 
		String stationtext = station + "-" + temp_type;
		String temperature_str = read_data[3];
		Double temperature;
		Double old_temperature;
		
		int station_count = 0;
		// Convert temperature string to double. 
		try {
			temperature = Double.parseDouble(temperature_str);
		} catch (Exception e) {
			temperature = 0.0;
		}

		// Check if the data structure already has the key (Station + Type )
		// If no, create a new entry in the datastructure and insert the value read from the record. 
		if (!station_hashmap.containsKey(stationtext)) {
			station_hashmap.put(stationtext, (new Averagecountwritable(new DoubleWritable(temperature),
					new IntWritable(1), new Text(temp_type))));
		}

		// Update existing record with the values from the record obtained above
		else {

			old_temperature = (Double) (station_hashmap.get(stationtext).temperature.get());
			station_count = (Integer) (station_hashmap.get(stationtext).count.get()) + 1;
			temperature = temperature + old_temperature;

			station_hashmap.put(stationtext, (new Averagecountwritable(new DoubleWritable(temperature),
					new IntWritable(station_count), new Text(temp_type))));
		}
	}

	// Once data is accumulated, emit all records by station Id to the context. 
	// While writing output to the context, the key should be stationId alone and not stationId + Type.
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {

		Set<String> hashkeys = station_hashmap.keySet();
		String actual_key;
		for (String key : hashkeys) {
			// Get only the stationId from the key
			actual_key = key.split("-")[0];
			// Write to context with StationId and corresponding value in datastructure.
			context.write(new Text(actual_key), station_hashmap.get(key));
		}
	}
}
