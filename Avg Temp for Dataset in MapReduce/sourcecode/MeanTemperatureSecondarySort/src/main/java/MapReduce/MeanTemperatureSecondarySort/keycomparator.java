package MapReduce.MeanTemperatureSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// This is used to group input key to the reducer first by stationId 
// and then by descending order of year.
public class keycomparator extends WritableComparator {

	// Constructor for this call. Call base class constructor too inside. 
	protected keycomparator() {
		super(stringpair.class, true);
	}

	// Implement this method to perform comparision based on station and year.
	public int compare(WritableComparable w1, WritableComparable w2) {
		stringpair sp1 = (stringpair) w1;
		stringpair sp2 = (stringpair) w2;

		// Compare station values first. If same, check the year value.
		// Else return as station are different.
		int cmp = sp1.station.compareTo(sp2.station);

		if (cmp != 0) {
			return cmp;
		}
		return sp1.year.compareTo(sp2.year); // Reverse the sorting order to sort by descending
	}
}
