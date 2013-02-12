package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class sorter extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new sorter(), args);
	    System.exit(res);
	  }

	public int run(String[] args) throws Exception {
		FileSystem fs = null;

		JobConf conf = new JobConf(getConf(), wordCount1.class);
		try{
			Sorter SeqSort = new Sorter (fs,
					Text.class,
					IntWritable.class,
					new Configuration()); 
			
			//SeqSort.sort(tempSeq, new Path(tempSeq.getParent().toString()+"/S"+tempSeq.getName()));// in ${hadoop.tmp.dir}
		    	//fs.delete(tempSeq);
		    	//Merge sorted file
		    }
		    catch(Exception e){e.printStackTrace();}
		    return 0;
	}
}

