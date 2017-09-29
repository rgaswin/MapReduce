package MapReduce.PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Node implements WritableComparable<Node> {

	// Private variable Declarations
	private DoubleWritable pagerank;
	private Text adjacency_list;
	private BooleanWritable isNode;
	
	// Public Methods to access the private variables. 
	public void setPageRank(DoubleWritable pagerank) {
        this.pagerank = pagerank;
    }
	// Public Method for getting pagerank
    public DoubleWritable getPageRank() {
        return this.pagerank;
    }
    // Public Method for setting Adjacency List.
    public void setAdjList(Text list) {
        this.adjacency_list = list; 
    }
    // Public Method for getting Adjacency List.
    public Text getAdjacencyList() {
        return this.adjacency_list;
    }
    // Public Method for setting object to be a Node object.
    public void setIsNodeObj(BooleanWritable isNodeObj) {
        this.isNode = isNodeObj;
    }
    // Public Method for finding if an object is a Node object.
    public BooleanWritable isNodeObj() {
        return this.isNode;
    }
	
	// Default constructor
	protected Node(){
		this.pagerank = new DoubleWritable();
		this.adjacency_list = new Text();
		this.isNode = new BooleanWritable();
	}
	// Set values sent by the program.
	protected Node(DoubleWritable rank, Text adj_list, BooleanWritable isNode){
		this.pagerank = rank;
		this.adjacency_list = adj_list;
		this.isNode = isNode;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.pagerank.write(out);
		this.adjacency_list.write(out);	
		this.isNode.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pagerank.readFields(in);
		this.adjacency_list.readFields(in);
		this.isNode.readFields(in);
	}

	@Override
	public int compareTo(Node o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
