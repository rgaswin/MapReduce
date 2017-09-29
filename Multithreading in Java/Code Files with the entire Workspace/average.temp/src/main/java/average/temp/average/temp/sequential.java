package average.temp.average.temp;


import java.util.HashMap;
import java.util.Set;

/* Function to calculate average of temperatures in a SEQUENTIAL version of
the program. In this version, there is a single thread executing the program
sequentially and returning the results.
The function returns the time for execution of average calculation. */
public class sequential {

	// Calculate the average temperature by station in a sequential execution.
	public static long calc_average_temp(String[] linesfromfile) {
		// Data structure for accumulating and storing the output.
		HashMap<String, Double[]> acc_structure = new HashMap<String, Double[]>();
		HashMap<String, Double> output = new HashMap<String, Double>();

		// Variable Declarations.
		String[] splitstring;
		Double temp = 0.0;
		Double[] running_sum_count;

		// Record the start time for program execution.
		long start_time = System.currentTimeMillis();
		// Iterate through each record one by one.
		for (String s : linesfromfile) {
			splitstring = s.split(",");
			String temperature = splitstring[3];
			String station = splitstring[0];

			// If the record is a TMAX temperature, process it.
			if (splitstring[2].equals("TMAX")) {
				try {
					temp = Double.parseDouble(temperature);
				} catch (Exception e) {
					temp = 0.0;
				}
				
				// If not key already present, create a new key in the datastructure 
				if (!acc_structure.containsKey(station)) {
					// Call fibonacci to slow down the process
					fibonacci(17);
					running_sum_count = new Double[2];
					running_sum_count[0] = temp;
					running_sum_count[1] = 1.0;
					acc_structure.put(station, running_sum_count);
				} 
				// Update the temperature in the datastructure with the same key
				else {
					// Call fibonacci to slow down the process
					fibonacci(17);
					try {
						running_sum_count = (Double[]) (acc_structure.get(station));
						running_sum_count[0] = running_sum_count[0] + temp;
						running_sum_count[1] = running_sum_count[1] + 1.0;
						acc_structure.put(station, running_sum_count);
					} catch (Exception e) {
						temp = 0.0;
					}
				}
			}
		}
		// Put the result to another datastructure for printing. 
		Set<String> keyset = acc_structure.keySet();
		for (String key : keyset) {
			running_sum_count = (Double[]) acc_structure.get(key);
			Double running_sum = running_sum_count[0];
			Double stncount = running_sum_count[1];
			output.put(key, running_sum / stncount);
		}
		long end_time = System.currentTimeMillis();

		// Print results to the console.
		print_to_stdout(output);
		
		// Return the execution time in this iteration.
		return (end_time - start_time);

	}

	// The following function has been referred from Code source :
	// http://sampleprogramz.com/java/fibonacci.php
	public static int fibonacci(int n) {
		if (n == 0)
			return 0;
		else if (n == 1)
			return 1;
		else
			return fibonacci(n - 1) + fibonacci(n - 2);
	}

	// Function to print results to the standard output console.
	public static void print_to_stdout(HashMap<String, Double> output_map) {
		Set<String> keyset = output_map.keySet();
		for (String key : keyset) {
			Double avg = (Double) output_map.get(key);
			System.out.println("The Average temperature in Sequential Execution for station " + key + " is : " + avg);
		}
	}
}
