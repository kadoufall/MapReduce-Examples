package sort.secondary;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    // 将输入流中的字节流数据转化为结构化数据
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    // 将结构化数据写入输出流
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public int hashCode() {
        return first + "".hashCode() + second + "".hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof IntPair)) {
            return false;
        }

        IntPair intPair = (IntPair) obj;
        return intPair.first == this.first && intPair.second == this.second;
    }

    public int compareTo(IntPair obj) {
        if (this.first != obj.first) {
            return this.first - obj.first;
        } else if (this.second != obj.second) {
            return this.second - obj.second;
        } else {
            return 0;
        }
    }


}
