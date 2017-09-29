package average.temp.average.temp;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

// This class is an implementation when no structure is locked and threads execute 
// without any synchronization in the code.
public class nolock {

	// A function to calculate average temperature when no lock is applied on the program
	public static long calc_average_temp(String[] lines) throws FileNotFoundException, UnsupportedEncodingException {
		// Divide the input file based on indexes
		int start = 0;
		int mid = lines.length / 2;
		int end = lines.length;
		
		// Capture the start time for program execution
		long start_time = System.currentTimeMillis();

		// Create accumulation structure for calculating the sum of temperatures
		HashMap<String,Double[]> running_sum = new HashMap<String,Double[]>();
		HashMap<String,Double> output = new HashMap<String,Double>();
		
		// Instantiate objects of a class that creates threads and implements the runnable interface.
		// Also divide the input file to two threads and call appropriate methods that start the 
		// thread.
		ThreadingClass n1 = new ThreadingClass(running_sum, output, lines, start, mid);
		ThreadingClass n2 = new ThreadingClass(running_sum, output, lines, mid, end - 1);
		n1.start_thread();
		n2.start_thread();
		
		// Calculate thread after the threads complete their individual execution.
		n1.calc_average(running_sum);
		
		// Caputre the end time for the program execution
		long end_time = System.currentTimeMillis();
		
		// Print the Average values computed to the console. 
		n1.print(output);
		
		// Return the time taken for the program execution in this iteration.
		return (end_time - start_time);
		
	}
}
