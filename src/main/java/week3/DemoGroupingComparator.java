package week3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DemoGroupingComparator extends WritableComparator {

    protected DemoGroupingComparator() {
        super(DemoWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DemoWritable o1 = (DemoWritable) a;
        DemoWritable o2 = (DemoWritable) b;

        return o1.getYearMonth().compareTo(o2.getYearMonth());
    }
}
