package MapReduce.MeanTemperatureSecondarySort;

import org.apache.hadoop.mapreduce.Partitioner;

// Source : Some part of this code has been taken from the slides in the Learning Modules
public class Partitionerclass extends Partitioner<stringpair, AveragecountwritableSecondary> {

	// Partition the keys by staion Id as opposed to (StationId + Year).
	// This way we make sure that all records with the same stationid will be processed in the same reducer. 
	@Override
	public int getPartition(stringpair key, AveragecountwritableSecondary value, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs(key.station.hashCode()) % numPartitions;
	}

}
