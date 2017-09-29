package MapReduce.PageRank;

import java.util.TreeMap;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Source code is designed based on the learning modules.
public class MapperTopK extends Mapper<Object, Text, NullWritable, stringdoublepair> {

	// Create A TreeMap to store key value pairs in ascending order.
	public TreeMap<Double, stringdoublepair> repToRecordMap;

	@Override
	public void setup(Context context) {
		// Initialize the TreeMap.
		repToRecordMap = new TreeMap<Double, stringdoublepair>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Local variable Declarations
		String current_page;
		Double pagerank = 0.0;

		// Split the value field to get Nodes and adjacent Node
		String[] line = value.toString().split("\t");
		if (line.length >= 2) {
			current_page = line[0];
			try {
				pagerank = Double.parseDouble(line[2].trim());
			} catch (Exception e) {
				pagerank = 0.0;
			}
			// create a new custom object with page name and page rank
			stringdoublepair pair = new stringdoublepair(current_page.trim(), pagerank);

			// Insert into TreeMap with the pagerank and custom object.
			repToRecordMap.put(pagerank, pair);

			// If TreeMap size exceeds 100 values, remove the first key.
			if (repToRecordMap.size() > 100) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}

		}
	}

	// Write all values stored in TreeMap to the reducer. 
	// Send Top 100 values from each Mapper to the reducer.
	// All values will be sent to be a single Reducer.
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (stringdoublepair t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}

}
