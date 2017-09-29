package average.temp.average.temp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * Hello world!
 *
 */

public class App {
	// This is the Entry point of the program
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		// Path to the CSV for the loader routine.
		System.out.println("Enter the path of the CSV file");
		
		// Accept the user input from the console. 
		Scanner scanner = new Scanner(System.in);
		String path = scanner.nextLine();
		scanner.close();
		
	//	String path = "/Users/aswingopalan/Documents/1912.csv";
		
		
		
		// Load file contents in a list.
		List<String> linesFromFile = loadLinesFromFile(path);
		
		// Convert the list to an Array of Strings.
		String[] lines = linesFromFile.toArray(new String[0]);
		long time_for_execution = 0;
		
		// List to store Execution times during various iterations. 
		ArrayList<Long> seq_exec_time = new ArrayList<Long>();
		ArrayList<Long> nolock_exec_time = new ArrayList<Long>();
		ArrayList<Long> coarselock_exec_time = new ArrayList<Long>();
		ArrayList<Long> finelock_exec_time = new ArrayList<Long>();
		ArrayList<Long> nosharing_exec_time = new ArrayList<Long>();
		
		
		// Sequential Execution performed 10 times
		for (int i = 0; i < 10; i++) {
			// Returns the time for execution
			time_for_execution = sequential.calc_average_temp(lines);
			// Store Execution in a list for each iteration.
			seq_exec_time.add(time_for_execution);
		}

		// Nolock Execution performed 10 times
		for (int i = 0; i < 10; i++) {
			// Returns the time for execution
			time_for_execution = nolock.calc_average_temp(lines);
			// Store Execution in a list for each iteration.
			nolock_exec_time.add(time_for_execution);
		}
		// Coarse Execution performed 10 times
		for (int i = 0; i < 10; i++) {
			// Returns the time for execution
			time_for_execution = coarse.calc_average_temp(lines);
			// Store Execution in a list for each iteration.
			coarselock_exec_time.add(time_for_execution);
		}
		// Finelock Execution performed 10 times
		for (int i = 0; i < 10; i++) {
			// Returns the time for execution
			time_for_execution = finelock.calc_average_temp(lines);
			// Store Execution in a list for each iteration.
			finelock_exec_time.add(time_for_execution);
		} 
		// Nosharing Execution performed 10 times
		for (int i = 0; i < 10; i++) {
			// Returns the time for execution
			time_for_execution = nosharing.calc_average_temp(lines);
			// Store Execution in a list for each iteration.
			nosharing_exec_time.add(time_for_execution);
		}

		// Print the average, smallest and highest execution time to the console for each version.
		Printlogtime(seq_exec_time, "Sequential");
		Printlogtime(nolock_exec_time, "Nolock");
		Printlogtime(coarselock_exec_time, "Coarse");
		Printlogtime(finelock_exec_time, "Fine lock");
		Printlogtime(nosharing_exec_time, "No Sharing");
	
	}

	// Loader routine that picks up the file from the specified path
	public static List<String> loadLinesFromFile(String path) {
		Path Filepath = Paths.get(path);
		try {
			List<String> lines = Files.readAllLines(Filepath);
			return lines;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	public static void Printlogtime(ArrayList<Long> arrlist, String list_type) {
		// Get the size of the list (usually 10 for 10 iterations)
		int total_count = arrlist.size();
		int begin_count = 0;
		long avg = 0;

		// Sort the collection (arraylist) based on execution time
		Collections.sort(arrlist);

		// Iterate the collection to print the smallest, highest and the average time of execution.
		for (Long time_info : arrlist) {
			avg = avg + time_info;

			if (begin_count == 0) {
				System.out.println("The smallest execution time " + " for " + list_type + " is : " + time_info);
			}

			else if (begin_count == total_count - 1) {
				System.out.println("The highest execution time " + " for " + list_type + " is : " + time_info);
				System.out.println("The Average execution time " + " for " + list_type + " is : " + avg / total_count);
			}
			begin_count = begin_count + 1;
		}
	}

}
