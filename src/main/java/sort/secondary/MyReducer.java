package sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<IntPair, IntWritable, Text, IntWritable> {
    private static final Text SEPARATOR = new Text("--------------------");
    private final Text first = new Text();

    @Override
    public void reduce(IntPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(SEPARATOR, null);
        first.set(String.valueOf(key.getFirst()));

        for (IntWritable val : values) {
            context.write(first, val);
        }

    }
}