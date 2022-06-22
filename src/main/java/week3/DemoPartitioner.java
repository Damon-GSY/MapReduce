package week3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DemoPartitioner extends Partitioner<DemoWritable, NullWritable> {
    @Override
    public int getPartition(DemoWritable demoWritable, NullWritable nullWritable, int i) {
        return (demoWritable.getYearMonth().hashCode() & Integer.MAX_VALUE) %  i;
    }
}
