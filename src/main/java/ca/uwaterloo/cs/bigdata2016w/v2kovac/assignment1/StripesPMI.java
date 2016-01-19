package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment1;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.map.HMapStIW;
import tl.lin.data.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  // 1st Mapper: emits (word, 1) for every word pair occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text KEY = new Text();
    public enum MyCounter { COUNTER_NAME };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int cnt = 0;
      Set<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);

      // Your code goes here...
      for(int i=0; i < words.length; i++) {
        KEY.set(words[i]);
        context.write(KEY,ONE);
      }

      Counter counter = context.getCounter(MyCounter.COUNTER_NAME);
      counter.increment(1L);
    }
  }

  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // 2nd Mapper: emit (Pair, 1)
  private static class MySecondMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private final static Text KEY = new Text();
    private final static HMapStIW MAP = new HMapStIW();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int cnt = 0;
      Set<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);

      // Your code goes here...
      for(int i=0; i < words.length; i++) {
        for (int j=0; j < words.length; j++) {
          if(j==i) continue;
          MAP.increment(words[j]);
        }
        KEY.set(words[i]);
        context.write(KEY, MAP);
        MAP.clear();
      }
    }
  }

  private static class MySecondCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context) 
        throws IOException, InterruptedException{

      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      while(iter.hasNext()) {
        map.plus(iter.next());
      }
      context.write(key, map);
    }
  }

  private static class MySecondReducer extends Reducer<Text, HMapStIW, PairOfStrings, DoubleWritable> {
    
    private static Map<String, Integer> wordCount = new HashMap<String, Integer>();
    private static PairOfStrings PAIR = new PairOfStrings();
    private static DoubleWritable PMI = new DoubleWritable();
    private static long totalLines;
    
    @Override
    public void setup(Context context) throws IOException{
      Configuration conf = context.getConfiguration();
      totalLines = conf.getLong("counter", 0L);

      try {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("intermediate/"));
        for (int i=0; i < status.length; i++) {
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath()),"UTF-8"));
          String line = br.readLine();
          while(line != null){
            String[] tokens = line.split("\\s+");
            if(tokens.length == 2){
              wordCount.put(tokens[0], Integer.parseInt(tokens[1]));
            }
            line = br.readLine();
          }
        }
      } catch (Exception e) {
        throw new IOException("FILE DOESN'T EXIST or CAN'T OPEN FILE");
      }
      
    }
    
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context ) 
        throws IOException, InterruptedException{

      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      while(iter.hasNext()) {
        map.plus(iter.next());
      }

      String leftTerm = key.toString();

      for(String curKey : map.keySet()){
        if (map.get(curKey) < 10) continue;
        PAIR.set(leftTerm, curKey);

        double probPair = (double)map.get(curKey) / (double)totalLines;
        double probLeft = (double)wordCount.get(leftTerm) / (double)totalLines;
        double probRight = (double)wordCount.get(curKey) / (double)totalLines;

        double pmi = Math.log10((double)probPair / ((double)probLeft * (double)probRight));

        PMI.set(pmi);
        context.write(PAIR, PMI);

      }

    }

  }

  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);
    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path("intermediate/"));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path("intermediate/");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    //JOB 2
    long mapCount = job.getCounters().findCounter(MyMapper.MyCounter.COUNTER_NAME).getValue();
    conf.setLong("counter", mapCount);
    Job job2 = Job.getInstance(conf);
    job2.setJobName(StripesPMI.class.getSimpleName() + " PMI");
    job2.setJarByClass(StripesPMI.class);

    job2.setNumReduceTasks(args.numReducers);
    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(HMapStIW.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MySecondMapper.class);
    job2.setCombinerClass(MySecondCombiner.class);
    job2.setReducerClass(MySecondReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(args.output);
    FileSystem.get(conf).delete(outputDir2, true);

    job2.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
