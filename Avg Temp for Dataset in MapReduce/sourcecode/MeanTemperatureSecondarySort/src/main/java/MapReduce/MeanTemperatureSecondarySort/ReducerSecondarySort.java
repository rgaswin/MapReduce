package MapReduce.MeanTemperatureSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer class for the program. It inherits from the default Reducer class from the framework. 
//Input Key Type to the Reducer : stringpair
//Input Key Value to the Reducer : AveragecountwritableSecondary
//Output Key Type to the Reducer : Text
//Output Key Value to the Reducer : Text
public class ReducerSecondarySort extends Reducer<stringpair, AveragecountwritableSecondary, Text, Text> {

	@Override
	public void reduce(stringpair key, Iterable<AveragecountwritableSecondary> values, Context context)
			throws IOException, InterruptedException {
		String type;

		// Local variables Declaration.
		Double mean_min_temp = 0.0;
		Double sum_min_temp = 0.0;
		int sum_min_count = 0;

		Double mean_max_temp = 0.0;
		Double sum_max_temp = 0.0;
		int sum_max_count = 0;

		int current_year = 0;
		int previous_year = 0;
		//boolean allow_entry = true;
		boolean first_time = true;

		String final_output = ",";

		// Iterate through all values accumalated for this station (As grouping
		// happens by staion)
		for (AveragecountwritableSecondary val : values) {

			// Get the current year value
			current_year = val.year.get();
			
			if(first_time == true){
				previous_year = current_year;
				first_time = false;
			}
			
			type = val.temperature_type.toString();
			
			// Allow to enter the loop first time for every new year.
			if ( current_year == previous_year) {
				// If temperature type is TMAX, then accumulate running sum and
				// count in local MAX variables
				if (type.equals("TMAX")) {
					sum_max_temp += val.temperature.get();
					sum_max_count += val.count.get();
				}
				// If temperature type is TMIN, then accumulate running sum and
				// count in local MIN variables
				if (type.equals("TMIN")) {
					sum_min_temp += val.temperature.get();
					sum_min_count += val.count.get();
				}
				
			}

			// If the current and previous year values are not same,
			// it means we have processed all records for that year (Secondary
			// Sort)

			// In that case write the running sum and count for the current year
			// to a temporary output string.
			else {
				// allow_entry = true;
				// calculate the min temperature value for this year
				if (sum_min_count != 0) {
					mean_min_temp = sum_min_temp / sum_min_count;
				}
				// calculate the max temperature value for this year
				if (sum_max_count != 0) {
					mean_max_temp = sum_max_temp / sum_max_count;
				}
				// write the output to a temporary string.
				final_output = final_output + " [ " + previous_year + "," + mean_min_temp.toString() + ","
						+ mean_max_temp.toString() + " ] ,";

				
				// Reset running sum and count for TMAX record to the current
				// value of the record.
				if (type.equals("TMAX")) {
					sum_max_temp = val.temperature.get();
					sum_max_count = val.count.get();
					sum_min_temp = 0.0;
					sum_min_count = 0;
				}
				// Reset running sum and count for TMIN record to the current
				// value of the record.
				if (type.equals("TMIN")) {
					sum_min_temp = val.temperature.get();
					sum_min_count = val.count.get();
					sum_max_temp = 0.0;
					sum_max_count = 0;
					
				}
				// Reset all values used for previous calculations
				else{
					sum_min_temp = 0.0;
					sum_min_count = 0;
					sum_max_temp = 0.0;
					sum_max_count = 0;
					mean_min_temp = 0.0;
					mean_max_temp = 0.0;
				}

			}

			// Update previous year to current year.
			previous_year = current_year;

		}

		String temp_output = GetTextForLastRecord(sum_min_count, sum_max_count, sum_min_temp, sum_max_temp);
		final_output = final_output + " [ " + current_year + " , " + temp_output + " ] "; 

		// Write the output string to the final output.
		context.write(key.station, new Text(final_output));

	}

	// A function that return a string after computing Mean MAX and Mean MIN
	// based on input.
	public String GetTextForLastRecord(int sum_min_count, int sum_max_count, Double sum_min_temp,
			Double sum_max_temp) {
		Double mean_min_temp = 0.0;
		Double mean_max_temp = 0.0;

		if (sum_min_count != 0) {
			mean_min_temp = sum_min_temp / sum_min_count;
		}

		if (sum_max_count != 0) {
			mean_max_temp = sum_max_temp / sum_max_count;
		}
		return mean_min_temp.toString() + "," + mean_max_temp.toString();
	}

}
