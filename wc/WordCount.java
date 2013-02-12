/*
 * Condition: All file names start with 'f' otherwise change Mapper output.collector key
 * To Run: $ ../bin/hadoop jar multiwc.jar wc.WordCount /tmp/in /tmp/out
 */
package wc;

	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.FileUtil;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapred.JobClient;
	import org.apache.hadoop.mapred.JobConf;
	import org.apache.hadoop.mapred.TextOutputFormat;
	import org.apache.hadoop.mapred.FileInputFormat;
	import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.util.Tool;
	import org.apache.hadoop.util.ToolRunner;

	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.List;

	//import org.apache.hadoop.mapred.RecordReader;
	//import org.apache.hadoop.mapred.Reporter;
	//import org.apache.hadoop.mapred.InputSplit;
	//import org.apache.hadoop.mapred.MultiFileSplit;
	//import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
	//import org.apache.hadoop.io.LongWritable;

	//import org.apache.hadoop.mapred.MultiFileInputFormat;
	import java.io.File;
	
public class WordCount extends Configured implements Tool  {
	  public static Configuration myconf;
	  
	  public static void main(String[] args) throws Exception{
		    int res = ToolRunner.run(new Configuration(), new WordCount(), args); 
		    System.exit(res);
	  }
	  
	  public int run(String[] args) { 
		  
		String[] fileNames= null; 
		
	    JobClient client = new JobClient();
	    myconf = this.getConf(); //this is WordCount
	    JobConf conf = new JobConf(myconf,WordCount.class);
	    conf.setJarByClass(getClass());//What will this do ?
	    /* Answer:
	     * The jar that you give on the command line is the jar that contains
	     *  your main function, used to submit your Hadoop job. The other jar,
	     *   which you set with the call to job.setJarByClass(), is the one 
	     *   that contains your mapper and reducer code. It is this this second
	     *    jar that Hadoop will copy to every node and run it there,
	     */
	    if (args.length < 1) {
            System.out.println("ERROR: Please include inputDirectory path ..");
            System.exit(1);
        }

	    conf.setMapperClass(WordCountMapper.class);
	    conf.setReducerClass(WordCountReducer.class);
	    //conf.setNumReduceTasks(0);
	    //conf.setCombinerClass(WordCountReducer.class);
	    
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(intermediate.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);//intermediate.class);// == by default MapOutKeys
	    
	    conf.setInputFormat(wcInputFormat.class); //Splits are files
	    conf.setOutputFormat(TextOutputFormat.class);
	 
	    wcInputFormat.addInputPath(conf, new Path(args[0]));
	    TextOutputFormat.setOutputPath(conf, new Path(args[1]));
	    
	    //Parse input directory to produce filePaths here -->
	    /*
	    FileSystem hdfs = FileSystem.get(new Configuration());	
	    File inputDir = new File(args[0]);
	    //if (inputDir.isDirectory())
	    	fileNames = inputDir.list();
	    
	    System.out.println("\nThe files inside the directory are ..\n");
	    for(int i=0 ; i<fileNames.length;i++)
	    	System.out.println(fileNames[i]+", ");
	    */
	    client.setConf(conf);
	    try { JobClient.runJob(conf); } catch (Exception e) { e.printStackTrace();}
	    /*
	    System.out.println("\nNumber of files in input = "+wc.wcInputFormat.numFiles);
	    for(int i=0 ;i<wc.wcInputFormat.numFiles; i++)
	    System.out.println("\nfile path ="+wc.wcInputFormat.filePaths.get(i)+'*'); 
	    */    
	    return 0;
	  }
	}

