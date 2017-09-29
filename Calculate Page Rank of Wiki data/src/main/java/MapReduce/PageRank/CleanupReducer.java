package MapReduce.PageRank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupReducer extends Reducer<Text, Node, Text, Text> {

	public void reduce(Text key, Iterable<Node> value, Context context) throws IOException, InterruptedException {
		// Set adjacency list to empty string
		String adj_list = "";
		boolean isdangling = true;

		// Iterate through node objects and see if the boolean value "IsNode" in Node object
		// is set to true. If it's true, recover the graph structure.
		for (Node node : value) {
			if (node.isNodeObj().get() == true) {
				adj_list = node.getAdjacencyList().toString();
				isdangling = false;
				break;
			}
		}
		// Increment the number of nodes count by 1 in the global counter. 
		context.getCounter(Driver.CustomCounter.NUM_OF_NODES).increment(1);
		
		// If a dangline node is found, write a new entry in the output file with [].
		// else output the node with its adj list to the output file.
		if (isdangling == true) {
			context.write(key, new Text("[]"));
		}
		else {
			context.write(key, new Text(adj_list));
		}
	}
}