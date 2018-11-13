package sort.secondary;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator implements RawComparator<IntPair> {

    public int compare(byte[] b1, int s1, int i1, byte[] b2, int s2, int i2) {
        return WritableComparator.compareBytes(b1, s1, Integer.SIZE / 8, b2, s2, Integer.SIZE / 8);
    }

    public int compare(IntPair o1, IntPair o2) {
        int first1 = o1.getFirst();
        int first2 = o2.getFirst();
        return first1 - first2;
    }
}