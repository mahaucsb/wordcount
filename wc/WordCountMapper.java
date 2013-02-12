package wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import java.util.*; 


public class WordCountMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, intermediate> {


  public void map(LongWritable key, Text value, //value = one file path eg./tmp/in/f5
      OutputCollector<Text, intermediate> output, Reporter reporter) throws IOException {
	  
	  /*  THE FOLLOWING WORKED  */
	  FSDataInputStream inA = null;
	  FSDataInputStream inB = null;
	  String contentA = new String() ;
	  String contentB = new String() ;
	  FileSystem hdfs = null;
	  String a,b;
	  int n1,n2;
	  
	  try{  
		    hdfs = FileSystem.get(new Configuration());	
		   }catch(Exception e) {value.set(value.toString()+"Mapper Error");}
  
		   	/* LOAD OTHER FILES */
		   //numFiles: is not initialized here nor filePaths from InputFormat, try instantiating!!
		   // <<< PROBLEM HERE >>> ... LOADING OTHER INPUT DIRECTORY FILE ??? FOR NOW ... EXAMPLE
		   String[] path = {"/tmp/in/f1","/tmp/in/f2","/tmp/in/f3","/tmp/in/f4",
				   			  "/tmp/in/f5","/tmp/in/f6","/tmp/in/f7","/tmp/in/f8"};
		   	for(int i=0;i<path.length;i++){
		   		/* OPEN/READ ANOTHER FILE */
		   			try{
		   				inA = hdfs.open(new Path(value.toString()));
		   				contentA = inA.readLine(); 		     				   
		   				inB = hdfs.open(new Path(path[i]));				   
		   				contentB = inB.readLine();
		   				inA.close();
		   				inB.close();
		   			}catch(Exception e){;}

			   /* CALCULATE SCORES BEWTEEN CURRENT AND OPENED FILE */
			   int score = 0;
			   StringTokenizer itr1 = new StringTokenizer(contentA); //CURRENT
			   StringTokenizer itr2 = new StringTokenizer(contentB); //OPENED: f3,f4
			      while (itr1.hasMoreTokens() && itr2.hasMoreTokens()) {			    	  			      
				   a = itr1.nextToken(); 
				   b = itr2.nextToken(); 
				   n1 = Integer.parseInt(a);
				   n2 = Integer.parseInt(b);
				   score=score+(n1*n2);
			      }
			      String key1 = value.toString().substring(value.toString().indexOf('f'));
			      String key2 = path[i].substring(path[i].indexOf('f'))+'\n';
			
			      output.collect(new Text(key1), new intermediate(score,key2)); 
			   }	   
  }
}