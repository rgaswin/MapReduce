package MapReduce.PageRank;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class ReducerTopK extends Reducer<NullWritable, stringdoublepair, Text, Text> {
	// Initialize a new TreeMap to store the top 100 values.
	public TreeMap<Double, String> repToRecordMap = new TreeMap<Double, String>();
	
	@Override
	public void reduce(NullWritable key, Iterable<stringdoublepair> values, Context context) throws IOException, InterruptedException {
		// Iterate through all custom objects as they all have a single key.
		for (stringdoublepair pair : values) {
			// extract page rank and name from the custom object.
			Double page_rank = pair.pagerank.get();
			String page_name = pair.page_name.toString();
			// Put the values to a TreeMap
			repToRecordMap.put(page_rank,page_name);
			// Since keys are stored in ascending order, remove the first key
			// after 100 records are records. 
			if (repToRecordMap.size() > 100) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		// Emit the top 100 records to an output file with pagename and pagerank value.
		for (Double page_rank : repToRecordMap.descendingMap().keySet()) {
			context.write(new Text(repToRecordMap.get(page_rank)), new Text(page_rank.toString()));
		}
	}
}
