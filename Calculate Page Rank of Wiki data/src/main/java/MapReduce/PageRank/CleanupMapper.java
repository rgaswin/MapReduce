package MapReduce.PageRank;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class CleanupMapper extends Mapper<Object, Text, Text, Node> {

	public HashMap<String, Integer> GetOriginalNodes;

	// Map function for the clean up job to calculate dangling nodes
	// and initial page rank value.
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		GetOriginalNodes = new HashMap<String, Integer>();

		// Local variable Declarations
		String current_page, adj_list;

		// Split the value field to get Nodes and adjacent Node
		String[] line = value.toString().split("\t");
		if (line.length >= 2) {
			current_page = line[0].trim();
			adj_list = line[1].trim();
			String adj_list_without_duplicates = "";

			if (adj_list.equals("[]")) {
				adj_list_without_duplicates = adj_list;
			}

			else {
				String[] adj_list_nodes = adj_list.substring(1, adj_list.length() - 1).split(",");
				// Initialize a node with a default page rank, pass its
				// adjacency
				// list and set node obj to true
				// Emit the graph structure to the reducer.
				adj_list_without_duplicates = "[";

				for (String node : adj_list_nodes) {
					if (!GetOriginalNodes.containsKey(node.trim()))
						GetOriginalNodes.put(node.trim(), 1);
					else
						GetOriginalNodes.put(node.trim(), 10);

					if (GetOriginalNodes.get(node.trim()) == 1) {
						adj_list_without_duplicates += node.trim() + ",";
					}
				}
				adj_list_without_duplicates = adj_list_without_duplicates.substring(0,
						adj_list_without_duplicates.length() - 1);
				adj_list_without_duplicates += "]";
			}

			Node graph_structure = new Node(new DoubleWritable(0.0), new Text(adj_list_without_duplicates.trim()),
					new BooleanWritable(true));
			context.write(new Text(current_page.trim()), graph_structure);

			// Calculate page rank values for all nodes in the adjacency list
			String[] adj_list_nodes_no_duplicates = adj_list_without_duplicates
					.substring(1, adj_list_without_duplicates.length() - 1).split(",");

			for (String page : adj_list_nodes_no_duplicates) {
				// Initialize a new node with the pagerank obtained from the
				// Mapper records
				// and set Node object to false.
				if (!page.trim().equals("")) {
					Node page_rank_node = new Node();
					page_rank_node.setPageRank(new DoubleWritable(0.0));
					page_rank_node.setIsNodeObj(new BooleanWritable(false));

					// Ignore any self reference before emitting page rank
					// contributions from this page.
					if (page.trim() != current_page.trim()) {
						context.write(new Text(page.trim()), page_rank_node);
					}
				}
			}
		}

	}
}
