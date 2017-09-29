package average.temp.average.temp;

import java.util.HashMap;
import java.util.Set;

//A class that implements the Runnable Interface. 
//This class creates the threads required for 
//processing the input file.
public class ThreadingClassCoarse implements Runnable {
	// variables specific to this class.
	private HashMap<String, Double[]> acc_structure;
	private HashMap<String, Double> output_hmap;
	private String[] inputdata;
	private int start_index;
	private int end_index;
	private Thread t;

	// Initialize values specific to this object
	public ThreadingClassCoarse(HashMap<String, Double[]> acc, HashMap<String, Double> output, String[] data, int start,
			int end) {
		acc_structure = acc;
		output_hmap = output;
		inputdata = data;
		start_index = start;
		end_index = end;
	}

	// Implement the runnable interface.
	public void run() {
		this.process_threads(this.inputdata, this.start_index, this.end_index);
	}

	// A custom function to start thread execution.
	public void start_thread() {
		t = new Thread(this);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// This method is called for each thread during its execution.
	public void process_threads(String[] linesfromfile, int start, int end) {
		// Lock the entire accumulation structure. This stops the other thread
		// from accessing the structure.
		synchronized (acc_structure) {
			// Variable Declarations.
			String[] splitstring;
			Double temp = 0.0;
			Double[] running_sum_count;

			// Iterate through each based on the input. Input is divided for the
			// threads
			for (int i = start; i < end; i++) {
				splitstring = linesfromfile[i].split(",");
				String temperature = splitstring[3];
				String station = splitstring[0];

				// If the record is a TMAX temperature, process it.
				if (splitstring[2].equals("TMAX")) {
					try {
						temp = Double.parseDouble(temperature);
					} catch (Exception e) {
						temp = 0.0;
					}
					// If not key already present, create a new key in the
					// datastructure
					if (!this.acc_structure.containsKey(station)) {
						// Call fibonacci to slow down the process
						this.fibonacci(17);
						running_sum_count = new Double[2];
						running_sum_count[0] = temp;
						running_sum_count[1] = 1.0;
						this.acc_structure.put(station, running_sum_count);
					}
					// Update the temperature in the datastructure with the same
					// key
					else {
						// Call fibonacci to slow down the process
						this.fibonacci(17);
						try {
							running_sum_count = (Double[]) (this.acc_structure.get(station));
							running_sum_count[0] = running_sum_count[0] + temp;
							running_sum_count[1] = running_sum_count[1] + 1.0;
							this.acc_structure.put(station, running_sum_count);
						} catch (Exception e) {
							temp = 0.0;
						}
					}
				}
			}
		}
	}
	// Calculate average after accumalating the results.
	public void calc_average(HashMap<String, Double[]> sum_of_temp) {
		Set<String> HashKeys = sum_of_temp.keySet();
		Double[] values = new Double[2];
		Double avg = 0.0;
		for (String key : HashKeys) {
			values = (Double[]) sum_of_temp.get(key);
			if (values[0] != null && values[1] != null) {
				avg = values[0] / values[1];
			} else {
				avg = 0.0;
			}
			output_hmap.put(key, avg);
		}
	}

	// The following function has been referred from Code source :
	// http://sampleprogramz.com/java/fibonacci.php
	public int fibonacci(int n) {
		if (n == 0)
			return 0;
		else if (n == 1)
			return 1;
		else
			return fibonacci(n - 1) + fibonacci(n - 2);
	}

	// Print the record to the console.
	public void print(HashMap<String, Double> output) {
		Set<String> keys = output.keySet();
		for (String key : keys) {
			System.out.println("The Average for key : " + key + " in Coarse Lock execution is : " + output.get(key));
		}
	}

}
