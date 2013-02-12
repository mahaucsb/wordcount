/*
 * simplest word count for experiments use only...
 */
package wc;
import org.apache.hadoop.mapred.JobStatus;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.JobID; 

public class wordCount1 extends Configured implements Tool {
 
	public static org.apache.hadoop.mapred.FileSplit[] s = null;
	
	public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
	
		FileSystem fs = null;
		Path path ;	
	@SuppressWarnings("deprecation")
	@Override
	public void configure(JobConf job){
		try{
			fs = FileSystem.get(job);
		}catch(Exception e){e.printStackTrace();}
		//String path= job.getJobLocalDir(); ///tmp/hadoop-Hadoop/mapred/local/taskTracker/Hadoop/jobcache/job_201105202144_0037/work
		path = job.getWorkingDirectory(); //hdfs://localhost:9000/user/Hadoop
	}
	  
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, one);
      }
      output.collect(new Text(path.toString()), one);
    }

  }

  public static class Reduce extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  static int printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public int run(String[] args) throws Exception {	
	JobConf conf = new JobConf(getConf(), wordCount1.class);
    JobID j = new JobID();
    conf.setJobName("WordCount yes !");    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MapClass.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    conf.setNumReduceTasks(0);
    List<String> other_args = new ArrayList<String>();
    if(args.length>2) return printUsage();
    
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        return printUsage();
      }
    }     
    
   FileInputFormat.setInputPaths(conf, other_args.get(0));
   FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
    
    JobClient.runJob(conf); 
    return 0;
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new wordCount1(), args);
    Configuration  conf = new Configuration();
    JobClient jobClient = new JobClient(new InetSocketAddress("jobTracker",9001),conf);
    jobClient.setConf(conf); // Bug in constructor, doesn't set conf.

    JobStatus[] js = jobClient.getAllJobs();
    
     for(int i=0;i<js.length;i++){
        // We only care about completed jobs.
    	 if(!js[i].isJobComplete()){
    		 continue;
    	 } 
    	 // Do stuff on jobStatus.    	 
    	System.out.println(i+","+js[i].getJobId());
    	System.out.println(i+","+js[i].getStartTime());
     }
    System.exit(res);
  }

}
