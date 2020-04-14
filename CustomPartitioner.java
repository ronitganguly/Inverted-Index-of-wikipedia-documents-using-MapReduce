
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
//Taken from https://www.javacodegeeks.com/2013/01/mapreduce-algorithms-secondary-sorting.html
public class CustomPartitioner extends Partitioner<CompositeKeyWritable, NullWritable>{

	@Override
	public int getPartition(CompositeKeyWritable frequencypair, NullWritable nullWritable, int numPartitions) {
		return Math.abs(frequencypair.getNaturalKey().hashCode() % numPartitions);
		}
}
