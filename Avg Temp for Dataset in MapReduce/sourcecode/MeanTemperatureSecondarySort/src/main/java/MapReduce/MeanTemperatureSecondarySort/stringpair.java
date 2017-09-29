package MapReduce.MeanTemperatureSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class stringpair implements WritableComparable<stringpair> {

	public IntWritable year;
	public Text station;

	// Default constructor for the type
	public stringpair() {
		this.year = new IntWritable();
		this.station = new Text();
	}
	// Constructor accepting values from the program. 
	public stringpair(int year, String station) {
		this.year = new IntWritable(year);
		this.station = new Text(station);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.year.write(out);
		this.station.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.year.readFields(in);
		this.station.readFields(in);
	}

	// A compare function to compare by station and then by year
	@Override
	public int compareTo(stringpair othervalue) {
		int cmp = this.station.compareTo(othervalue.station);

		if (cmp != 0) {
			return cmp;
		}

		return this.year.compareTo(othervalue.year);
	}
}
