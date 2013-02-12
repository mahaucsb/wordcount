package wc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.PriorityQueue;
import java.util.Comparator;

public class WordCountReducer extends MapReduceBase
    implements Reducer<Text,intermediate, Text, Text> { 

  public void reduce(Text key, Iterator<intermediate> values, //eg. key=f1, Iterator=[<f2,score2>,...<fn,scoreN>]
      OutputCollector<Text,Text> output, Reporter reporter) throws IOException { 

	final int k = 5; //Paper threshold
    int sum = 0;
    Comparator<intermediate> Comp = new myComparator();    
    PriorityQueue<intermediate> Q = new PriorityQueue<intermediate>(k,Comp);    
    
    while (values.hasNext()) {
    	intermediate value = (intermediate) values.next(); 
	
    	if(Q.size() < k ) { Q.add(new intermediate(value.getScore(),value.getFile())); }
    	else if (Comp.compare(value, Q.peek())>0) {
    		Q.remove();
    		Q.add(value);
    	}
    	else; //IGNORE  
    }
    output.collect(key,new Text(Q.toString()));
    /* IF ABOVE DOESN'T PRINT IN ORDER THEN USE THE FOLLOWING *
    while(Q.size()>0)
    	output.collect(key,new Text(Q.remove().toString()));
    */  
  }  
  
  
}
