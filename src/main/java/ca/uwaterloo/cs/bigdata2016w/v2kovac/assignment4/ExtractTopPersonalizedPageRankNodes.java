package ca.uwaterloo.cs.bigdata2016w.v2kovac.assignment4;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.queue.TopScoredObjects;
import tl.lin.data.pair.PairOfObjectFloat;

/**
 * <p>
 * Driver program for partitioning the graph.
 * </p>
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MapClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static ArrayList<TopScoredObjects<Integer>> top = new ArrayList<TopScoredObjects<Integer>>();
    private static ArrayList<Integer> intSources = new ArrayList<Integer>();
    private static int max;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      max = conf.getInt("Top", 0);

      String[] sources = conf.getStrings("sources");
      for(String source: sources) {
        top.add(new TopScoredObjects<Integer>(max));
        intSources.add(Integer.valueOf(source));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {

      int i =0;
      for(TopScoredObjects<Integer> list: top) {
        list.add(nid.get(), node.getPageRank(i));
        i++;
      }

    }

    @Override
    public void cleanup(Context context) throws IOException {
      //print results
      for(int i=0; i < intSources.size(); i++) {
        System.out.println("Source: " + intSources.get(i));

        TopScoredObjects<Integer> list = top.get(i);
        for(PairOfObjectFloat<Integer> p : list.extractAll()){
          int nodeid = p.getLeftElement();
          float pagerank = (float) Math.exp(p.getRightElement());
          System.out.println(String.format("%.5f %d", pagerank, nodeid));
        }
        System.out.println();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
  }

  public ExtractTopPersonalizedPageRankNodes() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("sources").create(SOURCES));
    options.addOption(OptionBuilder.withArgName("top").hasArg()
        .withDescription("top").create(TOP));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inPath = cmdline.getOptionValue(INPUT);
    String outPath = cmdline.getOptionValue(OUTPUT);
    int max = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input dir: " + inPath);
    LOG.info(" - output dir: " + outPath);

    Configuration conf = getConf();
    conf.setInt("Top", max);
    conf.setStrings("sources", sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getSimpleName() + ":" + inPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    FileInputFormat.setInputPaths(job, new Path(inPath));
    FileOutputFormat.setOutputPath(job, new Path(outPath));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapClass.class);

    FileSystem.get(conf).delete(new Path(outPath), true);

    job.waitForCompletion(true);

    return 0;
  }
}