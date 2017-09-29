package MapReduce.MeanTemperatureInMapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//A custom class that Implements the WritableComparable Interface to make it treat 
//as a hadoop supported type. 

public class Averagecountwritable implements WritableComparable<Averagecountwritable> {

	// Members of the class
	public DoubleWritable temperature; // contains the temperature data
	public IntWritable count; // contains the count of stations
	public Text temperature_type; // contains the type of Temperature data
									// ("TMAX or TMIN")

	// Default constructor for the class
	public Averagecountwritable() {
		this.temperature = new DoubleWritable();
		this.count = new IntWritable();
		this.temperature_type = new Text();
	}

	// Parametrized constructor accepting values from the program.
	public Averagecountwritable(DoubleWritable temp, IntWritable count, Text type) {
		this.temperature = temp;
		this.count = count;
		this.temperature_type = type;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.temperature.write(out);
		this.count.write(out);
		this.temperature_type.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.temperature.readFields(in);
		this.count.readFields(in);
		this.temperature_type.readFields(in);
	}

	// A method that is used for comparision of objects of the same type.
	@Override
	public int compareTo(Averagecountwritable othervalue) {
		int compare_value1 = this.temperature.compareTo(othervalue.temperature);
		int compare_value2 = this.count.compareTo(othervalue.count);
		int compare_value3 = this.temperature_type.compareTo(othervalue.temperature_type);

		if (compare_value1 == 0 && compare_value2 == 0 && compare_value3 == 0) {
			return 0;
		}
		return -1;
	}
}
