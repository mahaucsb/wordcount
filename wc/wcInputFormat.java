package wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.MultiFileInputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.io.LongWritable;
import java.net.URI;

//MultiFileInputFormat is complete except for one abstract method "getRecordReader"
//To use MultiFileInputFormat, one should extend it, to return a (custom) RecordReader
//MultiFileInputFormat uses MultiFileSplits for getSplits

//public class wcMultiFileInputFormat extends MultiFileInputFormat<LongWritable, Text>{
public class wcInputFormat extends FileInputFormat<LongWritable,Text>{
	
	public static int numFiles = 0;
	public static URI uri; 
	public static InputSplit[] spts;
	public static List<String> filePaths;
	
	//Subclasses implement following to construct RecordReader's for MultiFileSplit
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter){
		return (new myRecordReader((MultiFileSplit) split));
	}	

	public InputSplit[] getSplits(JobConf job, int numSplits)     throws IOException {
		   	
		 	/* Following = 1 because there is one input directory path
		 	 * 1 = FileInputFormat.getInputPaths(job).length; */ 		      
		 	
		 	Path[] paths = FileUtil.stat2Paths(listStatus(job));
		 	/* paths[0]: hdfs://localhost:9000/tmp/in/f1 */
		 	uri = paths[0].toUri();
		    List<MultiFileSplit> splits = new ArrayList<MultiFileSplit>(paths.length);
		    filePaths = new ArrayList<String>(paths.length); //paths.length = # of files
		    //error > numFiles = splits.get(2).toString(); //////
		    if (paths.length != 0) {
		    
		      numFiles= paths.length;	    
			  numSplits = numFiles*100;// This is just a hint, it still depends on file size
				  
		      long[] lengths = new long[paths.length]; // NumberOfFiles
		      long totLength = 0;
		      for(int i=0; i<paths.length; i++) {
		        FileSystem fs = paths[i].getFileSystem(job);
		        lengths[i] = fs.getContentSummary(paths[i]).getLength(); //length of one file-contents
		        totLength += lengths[i]; //number of chars in all files including '\n' --checked
		      }
		      			      
		      double avgLengthPerSplit = ((double)totLength) / numSplits;
		      long cumulativeLength = 0;

		      int startIndex = 0;

		      for(int i=0; i<numSplits; i++) {
		        int splitSize = findSize(i, avgLengthPerSplit, cumulativeLength
		            , startIndex, lengths);
		        if (splitSize != 0) {
		          // HADOOP-1818: Manage split only if split size is not equals to 0
		          Path[] splitPaths = new Path[splitSize];
		          long[] splitLengths = new long[splitSize];
		          				//souce ,  SrcPos      , dst    ,DstPos , length
		          System.arraycopy(paths, startIndex, splitPaths , 0, splitSize);
		          System.arraycopy(lengths, startIndex, splitLengths , 0, splitSize);
		          splits.add(new MultiFileSplit(job, splitPaths, splitLengths));
		          startIndex += splitSize;
		          for(long l: splitLengths) {
		            cumulativeLength += l;
		          }
		        }
		      }
		    }	
		    spts = 	splits.toArray(new MultiFileSplit[splits.size()]);//accessed by mapper
		    for(int i=0; i< numFiles; i++) {
		    	int j = spts[i].toString().indexOf(':');
		    	String path = spts[i].toString().substring(0, j);
		    	filePaths.add(path);
		    }
		    
		    return splits.toArray(new MultiFileSplit[splits.size()]);    
		  }
	 private int findSize(int splitIndex, double avgLengthPerSplit
		      , long cumulativeLength , int startIndex, long[] lengths) {
		    
		    if(splitIndex == lengths.length - 1)
		      return lengths.length - startIndex;
		    
		    long goalLength = (long)((splitIndex + 1) * avgLengthPerSplit);
		    long partialLength = 0;
		    // accumulate till just above the goal length;
		    for(int i = startIndex; i < lengths.length; i++) {
		      partialLength += lengths[i];
		      if(partialLength + cumulativeLength >= goalLength) {
		        return i - startIndex + 1;
		      }
		    }
		    return lengths.length - startIndex;
	 }
	 
}
