package sort.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class Main {
    public static void main(String[] args) throws Exception {
        // 任务输入路径
        final String INPUT_PATH = "hdfs://localhost:9000/sort";
        // 任务输出路径
        final String OUTPUT_PATH = "hdfs://localhost:9000/output_sort";

        // 读取MapReduce系统配置信息
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        // 设置一个作业对象
        Job job = Job.getInstance(conf, "SortApp");
        // 设置处理该作业对象的类
        job.setJarByClass(Main.class);

        /**
         * 设置处理Map步的类
         * 设置Map步输出的key类型的类
         * 设置Map步输出的value类型的类
         */
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        /**
         * 设置处理Reduce步的类
         * 设置Reduce步输出的key类型的类
         * 设置Reduce步输出的value类型的类
         */
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * 设置作业输入的数据类型
         * 设置作业输入数据的路径
         */
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(INPUT_PATH);
        FileInputFormat.addInputPath(job, inputPath);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(OUTPUT_PATH);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 执行作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
