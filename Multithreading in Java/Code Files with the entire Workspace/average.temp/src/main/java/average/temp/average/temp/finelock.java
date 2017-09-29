package average.temp.average.temp;


import java.util.concurrent.ConcurrentHashMap;

public class finelock {

	/* Function to calculate average of temperatures in a COARSE-LOCK version of
	   the program. In this version, the only specific keys in the accumalation_structure is locked
	   for synchronizing different threads.
	   The function returns the time for execution of average calculation. */
	public static long calc_average_temp(String[] lines) {
		// Record the start time for calculation in a variale
		long start_time = System.currentTimeMillis();
		// Variables to divide the input file based on Indexes.
		int start = 0;
		int mid = lines.length / 2;
		int end = lines.length;

		// Passing a Concorrunent HashMap from the program to different threads. The data structure 
		// takes care of locking individual keys for synchronization.
		ConcurrentHashMap<String, Double[]> running_sum = new ConcurrentHashMap<String, Double[]>();
		ConcurrentHashMap<String, Double> output = new ConcurrentHashMap<String, Double>();
		Threadingclassfine n1 = new Threadingclassfine(running_sum, output, lines, start, mid);
		Threadingclassfine n2 = new Threadingclassfine(running_sum, output, lines, mid, end - 1);

		// Start the threads
		n1.start_thread();
		n2.start_thread();
		
		// Calculate the average of the threads
		n1.calc_average(running_sum);
		
		// Record the end time of program execution
		long end_time = System.currentTimeMillis();
		
		// Print output to the console. 
		n1.print(output);
		
		// Return the program execution time.
		return (end_time - start_time);
	}
}
