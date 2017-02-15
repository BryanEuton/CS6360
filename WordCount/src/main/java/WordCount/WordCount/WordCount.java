package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	private HashSet<String> positiveHash= new HashSet<String>();
	private HashSet<String> negativeHash = new HashSet<String>();
	
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text positive = new Text("Total count of positive words:");
    private Text negative = new Text("Total count of negative words:");
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	URI[] files = context.getCacheFiles();
    	readFile(files[0], positiveHash, context);
    	readFile(files[1], negativeHash, context);
    	super.setup(context);
    }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        if(positiveHash.contains(word.toString().toLowerCase())){
        	context.write(positive, one);        	
        }else if(negativeHash.contains(word.toString().toLowerCase())){
        	context.write(negative, one);        	
        }
        
      }
    }
    private void readFile(URI uri, HashSet hash, Context context) throws IOException {
    	BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));
		String s;
		while((s= bufferedReader.readLine()) != null) {
			if(!s.startsWith(";") && !s.trim().isEmpty()){
        	  hash.add(s.trim().toLowerCase());
          }
		}
	}

  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      String k = key.toString();
      Text out;
      if(k.equals("positive") || k.equals("negative")){
    	  out = new Text("Total count of " + key.toString() + " words:");
      }else{
    	  out = key;
      }
      context.write(out, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.addCacheFile(new Path(args[2].toString()).toUri());
    job.addCacheFile(new Path(args[3].toString()).toUri());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}