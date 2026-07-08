import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesSum {

  public static class SalesMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text customerId = new Text();
    private DoubleWritable amount = new DoubleWritable();

    public void map(Object key, Text value, Context context
                     ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] parts = line.split(",");
      if (parts.length == 2) {
        customerId.set(parts[0]);
        amount.set(Double.parseDouble(parts[1]));
        context.write(customerId, amount);
      }
    }
  }

  public static class SalesReducer
       extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      double sum = 0.0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sales sum");
    job.setJarByClass(SalesSum.class);
    job.setMapperClass(SalesMapper.class);
    job.setReducerClass(SalesReducer.class);
    job.setNumReduceTasks(4);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
