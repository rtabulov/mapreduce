package com.mycompany.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAverage {
  public static float average(Float[] values) {
    int length = values.length;
    float sum = 0;
    for (int i = 0; i < length; i++) {
      sum += values[i];
    }
    return sum / (float) length;
  }

  public static class MonthMapper
      extends Mapper<Object, Text, Text, FloatWritable> {
    private Text word = new Text();
    private FloatWritable result = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] split = value.toString().split(",");
      String date = split[0];
      Float maxTemp = Float.parseFloat(split[2]);

      String month = date.substring(2, 4);
      String year = date.substring(4);

      String mapKey = month.concat(year);

      word.set(mapKey);
      result.set(maxTemp);
      context.write(word, result);
    }
  }

  public static class FloatAvgReducer
      extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
        Context context) throws IOException, InterruptedException {
      float sum = 0;
      int len = 0;

      for (FloatWritable val : values) {
        sum += val.get();
        len++;
      }

      result.set(sum / len);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "weather average");
    job.setJarByClass(WeatherAverage.class);
    job.setMapperClass(MonthMapper.class);
    job.setCombinerClass(FloatAvgReducer.class);
    job.setReducerClass(FloatAvgReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
