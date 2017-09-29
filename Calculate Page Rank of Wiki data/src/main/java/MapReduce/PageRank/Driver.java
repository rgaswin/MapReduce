package MapReduce.PageRank;

import java.io.IOException;

public class Driver {

	// Create an enum, to create global counter variables. 
	public enum CustomCounter {
		CURRENT_DELTA,
		PREVIOUS_DELTA,
		NUM_OF_NODES
	}
	
	
	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		// Start the pre processing job and return the output file path
		String output_path_job1 = ParseAndPreprocessConfig.runjob(args);
		String output_path_job2 = output_path_job1 + "_2";
		
		// Start the cleanup job to identify dangling nodes and updating them to the input
		// before using it for page rank calculations. 
		// Capture the number of nodes returned from this job. will be used for initial page rank
		// calculations.
		long num_of_nodes = CleanupConfig.runjob(output_path_job1, output_path_job2);

		// Use this variable to change the input file path for next iteration.
		String prev_iter_outputpath = output_path_job2;
		// Initialize delta to 0 and use it in first iteration. 
		Double delta = 0.0;
		
		// Run the page rank job 10 times. 
		for (int i = 1; i <= 10; i++) {
			// Capture new delta value and pass it to the next iteration.. 
			delta = PageRankCalculationConfig.runjob(prev_iter_outputpath, output_path_job2 + "_" + i, num_of_nodes , delta);
			// update input path for next iteration.
			prev_iter_outputpath = output_path_job2 + "_" + i;
		}
		// Input and output path for the Top-K job.
		String output_path_job10 = prev_iter_outputpath;
		String output_path_job11 = output_path_job2 + "_" + "final";
		// Run the Top-K Job. 
		TopKConfig.runJob(output_path_job10, output_path_job11);
		
	}
}