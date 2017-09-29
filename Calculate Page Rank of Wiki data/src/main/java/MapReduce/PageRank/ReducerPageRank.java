package MapReduce.PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

//REDUCER : A reducer class that outputs pages with their new page ranks after calculations.
// Input Key - Text (Page Name)
// Input Value - Node (Node object associated to the page)
// Output Key - Text (Page Name)
// Output Value - Text (Graph Structure + New Page Rank)
public class ReducerPageRank extends Reducer<Text, Node, Text, Text> {

	// Class variables
	public long num_of_nodes;
	public Configuration conf;
	public double delta_previous_iter;

	// Initialize class variables in setup method. 
	public void setup(Context context) {
		conf = context.getConfiguration();
		num_of_nodes = conf.getLong("NUM_OF_NODES", 1);
		delta_previous_iter = conf.getDouble("PREVIOUS_DELTA", 0.0);
	}

	// The Reduce function that performs the reduce operation.
	public void reduce(Text key, Iterable<Node> value, Context context) throws IOException, InterruptedException {
		// Variable Declarations
		Double damping_factor = 0.15;
		Double page_rank_total = 0.0;
		Double final_pagerank = 0.0;
		
		String adj_list = "";

		// Iterate through all Nodes to get the running sum of page rank
		// contribution for this page and also recover the graph structure for this page.	
		for (Node node : value) {
			if (node.isNodeObj().get() == true) {
				adj_list = node.getAdjacencyList().toString();
			} else {
				page_rank_total += node.getPageRank().get();
			}
		}
		
		// Calculate page rank for this page using the formula given.
		// Add delta contrubitions from the previous iteration if available.
		final_pagerank = (damping_factor / num_of_nodes)
				+ ((1 - damping_factor) * (page_rank_total + (delta_previous_iter / num_of_nodes)));

		// Emit page as key
		// Emit Adj_list + Computed Pagerank to a output file. 
		// This file will be used in the next iteration as input.
		context.write(key, new Text(adj_list + "\t" + final_pagerank));
	}
}
