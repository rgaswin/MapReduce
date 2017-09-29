package average.temp.average.temp;

import java.util.HashMap;
import java.util.Set;

/* Function to calculate average of temperatures in a NO SHARING version of the program.
In this version, threads execute independently without a shared data structure and the results of 
individual threads are gathered in the end and combined.
The function returns the time for execution of average calculation. */

public class nosharing {
	public static long calc_average_temp(String[] lines) {
		// Record start time for the program execution
		long start_time = System.currentTimeMillis();
		
		// Index caluclation for Input file split 
		int start = 0;
		int mid = lines.length / 2;
		int end = lines.length;

		// Instantiate objects with values that divide the input file equally among 
		// the threads. Do not use a common data structure in this version.
		Threadclassnosharing n1 = new Threadclassnosharing(lines, start, mid);
		Threadclassnosharing n2 = new Threadclassnosharing(lines, mid, end - 1);
		// Start the threads.
		n1.start_thread();
		n2.start_thread();

		// Compute the average by joinint the results of the threads.
		HashMap<String, Double> finalAvg = new HashMap<String, Double>();

		Set<String> thread1_keys = n1.acc_structure.keySet();
		Set<String> thread2_keys = n2.acc_structure.keySet();

		// Iterate through all the keys in the data structure specific to each thread.
		// If both threads share the same key, combine the results of the two threads
		// and compute average
		for (String key : thread1_keys) {
			if (n1.acc_structure.get(key) != null && n2.acc_structure.get(key) != null) {
				Double[] thread1_sum_count = (Double[]) n1.acc_structure.get(key);
				Double[] thread2_sum_count = (Double[]) n2.acc_structure.get(key);
				Double t1_map_sum = thread1_sum_count[0];
				Double t2_map_sum = thread2_sum_count[0];
				Double t1_map_count = thread1_sum_count[1];
				Double t2_map_count = thread2_sum_count[1];
				finalAvg.put(key, (t1_map_sum + t2_map_sum) / (t1_map_count + t2_map_count));
			}

			// Compute average for keys specific to only one thread
			if (n1.acc_structure.get(key) != null && n2.acc_structure.get(key) == null) {
				Double[] running_sum_count = (Double[]) (n1.acc_structure.get(key));
				Double sum = running_sum_count[0];
				Double count = running_sum_count[1];
				finalAvg.put(key, sum / count);
			}
		}

		// Compute average for keys specific to the other thread
		for (String key : thread2_keys) {
			if (n1.acc_structure.get(key) == null && n2.acc_structure.get(key) != null) {
				Double[] running_sum_count = (Double[]) (n2.acc_structure.get(key));
				Double sum = running_sum_count[0];
				Double count = running_sum_count[1];
				finalAvg.put(key, sum / count);
			}
		}
		// Record the end time 
		long end_time = System.currentTimeMillis();

		// Print output to the console.
		n1.print(finalAvg);

		// Return the program execution time in this iteration.
		return (end_time - start_time);
	}
}
