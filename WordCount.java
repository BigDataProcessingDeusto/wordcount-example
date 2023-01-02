import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
  /*
    TokenizerMapper class. It extends the Hadoop MR Mapper class.
    <Object, Text, Text, IntWritable> represent the type of the
    input/output key-value pairs. Our map function will
    receive a key-value pair of type Object-Text and will return
    a new key-value pair of type Text-IntWritable.
  */

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    // IntWritable: the Hadoop flavour of Integer and String, which are 
    // optimized to provide serialization in Hadoop
    private final static IntWritable one = new IntWritable(1);

    private Text word = new Text();
    // map function --> Input key and value and MR context
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // A function to tokenize text
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  /*
    IntSumReducer class. It extends Hadoop MR Reducer class.
    <Text,IntWritable,Text,IntWritable> represent the type of the
    input/output key-value pairs. Our reduce function will
    receive a key-value pair of type Text-IntWritable and will return
    a new key-value pair of type Text-IntWritable.
  */

  public static class IntSumReducer
       extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    // reduce function --> Input key and value and MR context
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
