package week3;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DemoWritable implements Writable, WritableComparable<DemoWritable> {
    private String yearMonth;
    private int temperature;

    //1. 空构造器
    public DemoWritable() {};

    //2. 有参数构造器
    public DemoWritable(String yearMonth, int temperature) {
        this.yearMonth = yearMonth;
        this.temperature = temperature;
    }

    // 3. getter and setter

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth = yearMonth;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(yearMonth);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth = dataInput.readUTF();
        temperature = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "DemoWritable{" +
                "yearMonth='" + yearMonth + '\'' +
                ", temperature=" + temperature +
                '}';
    }

    @Override
    public int compareTo(DemoWritable o) {
        int result = this.yearMonth.compareTo(o.getYearMonth());
        if (result == 0) {
            result = Integer.compare(this.temperature, o.getTemperature());
        }
        return result;
    }
}
