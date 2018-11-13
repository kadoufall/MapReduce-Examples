package sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
    // 输出的key类型
    private final IntPair key = new IntPair();
    // 输出的value类型
    private final IntWritable value = new IntWritable();

    @Override
    public void map(LongWritable inKey, Text inValue, Context context)
            throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(inValue.toString());
        if (st.hasMoreTokens()) {
            int first = Integer.parseInt(st.nextToken());
            int second = Integer.parseInt(st.nextToken());
            key.set(first, second);
            value.set(second);
            context.write(key, value);
        }
    }

}