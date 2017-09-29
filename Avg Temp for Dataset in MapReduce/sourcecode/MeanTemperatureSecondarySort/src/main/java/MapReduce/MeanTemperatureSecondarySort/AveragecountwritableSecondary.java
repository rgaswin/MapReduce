package MapReduce.MeanTemperatureSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//A custom class that Implements the WritableComparable Interface to make it treat 
//as a hadoop supported type. 

public class AveragecountwritableSecondary implements WritableComparable<AveragecountwritableSecondary> {

	// Members of the class
	public IntWritable year; // Contains the year the data was collected
	public DoubleWritable temperature; // Contains the temperature value of a station
	public IntWritable count; // Contains the count of records processed for a station. 
	public Text temperature_type; // Contains the type of the Temperature ("TMAX" or "TMIN" )

	// Default constructor for the class
	public AveragecountwritableSecondary() {
		this.year = new IntWritable();
		this.temperature = new DoubleWritable();
		this.count = new IntWritable();
		this.temperature_type = new Text();
	}

	// Parametrized constructor accepting values from the program.
	public AveragecountwritableSecondary(IntWritable year, DoubleWritable temp, IntWritable count, Text type) {
		this.year = year;
		this.temperature = temp;
		this.count = count;
		this.temperature_type = type;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.year.write(out);
		this.temperature.write(out);
		this.count.write(out);
		this.temperature_type.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.year.readFields(in);
		this.temperature.readFields(in);
		this.count.readFields(in);
		this.temperature_type.readFields(in);
	}

	// A method that is used for comparision of objects of the same type.
	@Override
	public int compareTo(AveragecountwritableSecondary othervalue) {
		int compare_value0 = this.year.compareTo(othervalue.year);
		int compare_value1 = this.temperature.compareTo(othervalue.temperature);
		int compare_value2 = this.count.compareTo(othervalue.count);
		int compare_value3 = this.temperature_type.compareTo(othervalue.temperature_type);

		if (compare_value0 == 0 && compare_value1 == 0 && compare_value2 == 0 && compare_value3 == 0) {
			return 0;
		}
		return -1;
	}
}
