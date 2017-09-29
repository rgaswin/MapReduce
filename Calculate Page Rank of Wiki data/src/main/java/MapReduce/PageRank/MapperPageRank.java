package MapReduce.PageRank;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

// A Mapper class that calculates page rank until all page rank values converge. 
//Input Key - Object (Page Name)
//Input Value - Text (A single line of input from file)
//Output Key - Text (Page Name)
//Output Value - Node (Node object associated to the page)
public class MapperPageRank extends Mapper<Object, Text, Text, Node> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Retrieve the current configuration for this job from context.
		Configuration conf = context.getConfiguration();

		// Local variable Declarations
		String current_page, adj_list;
		Double pagerank = 0.0;
		// Get number of nodes from the configuration object set in this job.
		long num_of_nodes = conf.getLong("NUM_OF_NODES", 1);
		// Assign initial page rank of 1/ total number of nodes
		Double initial_pagerank = 1.0 / num_of_nodes;
		
		// Split the value field to get Nodes and adjacent Node
		String[] line = value.toString().split("\t");
		if (line.length >= 2) {
			current_page = line[0];
			adj_list = line[1];
			// Initialize a node with a default page rank, pass its adjacency list 
			// and set node obj to true. Emit the graph structure to the reducer.
			Node graph_structure = new Node(new DoubleWritable(0.0), new Text(adj_list), new BooleanWritable(true));
			context.write(new Text(current_page), graph_structure);

			// From the second Iteration, the input file will have a page rank
			// value.
			if (line.length == 3) {
				try {
					pagerank = Double.parseDouble(line[2]);
				} catch (Exception e) {
					pagerank = 0.0;
				}
			}
			// If its the First iteration for this job, initialize it to the
			// default page rank value.
			if (pagerank == 0.0) {
				pagerank = initial_pagerank;
			}

			// Update dangling node contributions to a global counter
			// that will be used in the next iteration.
			if (adj_list.trim().equals("[]")) {
			    // Retrieve the current delta value
				// Convert Long to Double in such a way to preserve the double value. 
				Double delta = context.getCounter(Driver.CustomCounter.CURRENT_DELTA).getValue() / Math.pow(10, 10);
		    	delta += pagerank;
				long delta_long_val = (long) (delta * Math.pow(10, 10));
				// Update global counter with the latest delta value.
				context.getCounter(Driver.CustomCounter.CURRENT_DELTA).setValue((delta_long_val));
			}

			else {
				if (adj_list.length() > 2) {
					String[] adj_list_nodes = adj_list.substring(1, adj_list.length() - 1).split(",");
					int adj_list_length = adj_list_nodes.length;
					// page rank contribution to be sent to outlinks in adjacency list
					Double page_rank_contribution = pagerank / adj_list_length;

					for (String page : adj_list_nodes) {
						// Initialize a new node with its page rank contribution and 
						// do not send the graph structure for these nodes.
						Node page_rank_node = new Node();
						page_rank_node.setPageRank(new DoubleWritable(page_rank_contribution));
						page_rank_node.setIsNodeObj(new BooleanWritable(false));

						// Ignore any self reference before emitting page rank
						// contributions from this page.
						if (page != current_page) {
							// Emit page and its node object
							context.write(new Text(page), page_rank_node);
						}
					}
				}
			}
		}
	}
}
