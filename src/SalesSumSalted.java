import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesSumSalted {

  // ---- Phase 1: salt the key, partial-sum per salted key ----
  public static class SaltMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text saltedKey = new Text();
    private DoubleWritable amount = new DoubleWritable();
    private Random rand = new Random();
    private static final int SALT_BUCKETS = 10;

    public void map(Object key, Text value, Context context
                     ) throws IOException, InterruptedException {
      String[] parts = value.toString().split(",");
      if (parts.length == 2) {
        int salt = rand.nextInt(SALT_BUCKETS);
        saltedKey.set(parts[0] + "_" + salt);
        amount.set(Double.parseDouble(parts[1]));
        context.write(saltedKey, amount);
      }
    }
  }

  public static class PartialSumReducer
       extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      double sum = 0.0;
      for (DoubleWritable val : values) sum += val.get();
      result.set(sum);
      context.write(key, result);
    }
  }

  // ---- Phase 2: strip salt suffix, sum partials into true total ----
  public static class StripSaltMapper
       extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text customerId = new Text();
    private DoubleWritable amount = new DoubleWritable();

    public void map(Object key, Text value, Context context
                     ) throws IOException, InterruptedException {
      // input format: "CUST_HOT_3\t1234.56"
      String[] parts = value.toString().split("\\s+");
      if (parts.length == 2) {
        String saltedKey = parts[0];
        int lastUnderscore = saltedKey.lastIndexOf('_');
        String originalKey = saltedKey.substring(0, lastUnderscore);
        customerId.set(originalKey);
        amount.set(Double.parseDouble(parts[1]));
        context.write(customerId, amount);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // args: <input> <intermediate-output> <final-output>
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "sales sum salted phase1");
    job1.setJarByClass(SalesSumSalted.class);
    job1.setMapperClass(SaltMapper.class);
    job1.setCombinerClass(PartialSumReducer.class);
    job1.setReducerClass(PartialSumReducer.class);
    job1.setNumReduceTasks(4);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    boolean success1 = job1.waitForCompletion(true);
    if (!success1) System.exit(1);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "sales sum salted phase2");
    job2.setJarByClass(SalesSumSalted.class);
    job2.setMapperClass(StripSaltMapper.class);
    job2.setCombinerClass(PartialSumReducer.class);
    job2.setReducerClass(PartialSumReducer.class);
    job2.setNumReduceTasks(4);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
