package sort.basic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    // 输出的value类型
    private static IntWritable data = new IntWritable(1);

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val : values) {
            context.write(key, data);
        }

        data = new IntWritable(data.get() + 1);

    }
}
