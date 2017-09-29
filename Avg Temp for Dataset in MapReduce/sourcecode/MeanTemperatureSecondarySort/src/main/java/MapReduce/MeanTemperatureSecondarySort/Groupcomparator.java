package MapReduce.MeanTemperatureSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// The class implements the Grouping comparator to group 
// records sent to the reducer for processing. 
// It extends a WritableComparator class. 
public class Groupcomparator extends WritableComparator {

	// Constructor. call the base class constructor using super.
	protected Groupcomparator() {
		super(stringpair.class, true);
	}

	// Method to compare similar types. 
	public int compare(WritableComparable w1, WritableComparable w2) {
		stringpair sp1 = (stringpair) w1;
		stringpair sp2 = (stringpair) w2;
		int cmp = sp1.station.compareTo(sp2.station);
		return cmp; 
	}
}

