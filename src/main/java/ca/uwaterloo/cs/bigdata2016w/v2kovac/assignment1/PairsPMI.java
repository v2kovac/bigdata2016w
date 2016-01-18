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

import tl.lin.data.pair.PairOfStrings;

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

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
  private static class MySecondMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final static PairOfStrings PAIR = new PairOfStrings();

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
        for (int j=i+1; j < words.length; j++) {
          PAIR.set(words[i],words[j]);
          context.write(PAIR,ONE);
        }
      }
    }
  }

  private static class MySecondCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static IntWritable SUM = new IntWritable(); 
    
    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException{
      int sum = 0;
      for(IntWritable value : values){
        sum += value.get();
      }
      SUM.set(sum);
      context.write(pair, SUM);
    }
  }

  private static class MySecondReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
    
    private static Map<String, Integer> wordCount = new HashMap<String, Integer>();
    private static DoubleWritable PMI = new DoubleWritable();
    private static long totalLines;
    
    @Override
    public void setup(Context context) throws IOException{
      Configuration conf = context.getConfiguration();
      totalLines = conf.getLong("counter", 0L);

      FileSystem fs = FileSystem.get(conf);
      
      Path inFile = new Path("/u8/v2kovac/cs489/bigdata2016w/intermediate/part-r-00000");

      if(!fs.exists(inFile)){
        throw new IOException("File Not Found: " + inFile.toString());
      }
      
      BufferedReader reader = null;
      try{
        FSDataInputStream in = fs.open(inFile);
        InputStreamReader inStream = new InputStreamReader(in);
        reader = new BufferedReader(inStream);
        
      } catch(FileNotFoundException e){
        throw new IOException("Exception thrown when trying to open file.");
      }
      
      
      String line = reader.readLine();
      while(line != null){
        String[] parts = line.split("\\s+");
        if(parts.length != 2){
          LOG.info("Input line did not have exactly 2 tokens: '" + line + "'");
        } else {
          wordCount.put(parts[0], Integer.parseInt(parts[1]));
        }
        line = reader.readLine();
      }
      
      reader.close();
      
    }
    
    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context ) 
        throws IOException, InterruptedException{

      int sumOfPair = 0;
      for (IntWritable value : values) {
        sumOfPair += value.get();
      }
      
      if (sumOfPair >= 10){
        String left = pair.getLeftElement();
        String right = pair.getRightElement();

        double probPair = (double)sumOfPair / (double)totalLines;
        double probLeft = (double)wordCount.get(left) / (double)totalLines;
        double probRight = (double)wordCount.get(right) / (double)totalLines;

        double pmi = Math.log10((double)probPair / ((double)probLeft * (double)probRight));

        pair.set(left, right);
        PMI.set(pmi);
        context.write(pair, PMI);
      }

    }

  }

  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path("/u8/v2kovac/cs489/bigdata2016w/intermediate/"));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path("/u8/v2kovac/cs489/bigdata2016w/intermediate/");
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    //JOB 2
    long mapCount = job.getCounters().findCounter(MyMapper.MyCounter.COUNTER_NAME).getValue();
    conf.setLong("counter", mapCount);
    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName() + "PMI");
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(IntWritable.class);
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
    ToolRunner.run(new PairsPMI(), args);
  }
}
