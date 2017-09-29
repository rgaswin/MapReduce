package MapReduce.PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class stringdoublepair implements WritableComparable<stringdoublepair> {

	public Text page_name;
	public DoubleWritable pagerank;

	// Default constructor for the type
	public stringdoublepair() {
		this.page_name = new Text();
		this.pagerank = new DoubleWritable();
	}

	// Constructor accepting values from the program.
	public stringdoublepair(String dummy, double rank) {
		this.page_name = new Text(dummy);
		this.pagerank = new DoubleWritable(rank);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.page_name.write(out);
		this.pagerank.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Let the default types implement their own write methods for their
		// types
		this.page_name.readFields(in);
		this.pagerank.readFields(in);
	}

	// A compare function to compare by station and then by year
	@Override
	public int compareTo(stringdoublepair othervalue) {
		// Compare the pagerank values first.
		int cmp = this.pagerank.compareTo(othervalue.pagerank);
		// If page rank are not equal, return the value
		if (cmp != 0) {
			return cmp;
		}
		// Compare by pagename if pank ranks are not equal.
		return this.page_name.compareTo(othervalue.page_name);
		
	}
}
