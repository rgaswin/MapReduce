package average.temp.average.temp;

import java.util.HashMap;

public class coarse {
	/* Function to calculate average of temperatures in a COARSE-LOCK version of the program.
	   In this version, the whole datastructe (accumalation_structure) is locked for synchronizing 
	   different threads.
	   The function returns the time for execution of average calculation. */
	public static long calc_average_temp(String[] lines) {
		// Record the start time for calculation in a variale
		long start_time = System.currentTimeMillis();
		
		// Variables to divide the input file based on Indexes.
		int start = 0;
		int mid = lines.length / 2;
		int end = lines.length;

		// Passing the same accumulation structure from the program to different threads.
		HashMap<String, Double[]> running_sum = new HashMap<String, Double[]>();
		HashMap<String, Double> output = new HashMap<String, Double>();
		ThreadingClassCoarse n1 = new ThreadingClassCoarse(running_sum, output, lines, start, mid);
		ThreadingClassCoarse n2 = new ThreadingClassCoarse(running_sum, output, lines, mid, end - 1);
		// Start the thread
		n1.start_thread();
		n2.start_thread();
		// Calculate average
		n1.calc_average(running_sum);
		// Capture the end time for this execution.
		long end_time = System.currentTimeMillis();
		// Print the results to stdout.
		n1.print(output);

		// Return the time for program execution
		return (end_time - start_time);
	}
}
