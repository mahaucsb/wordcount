package wc;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import java.util.Vector;

public class myRecordReader implements RecordReader<LongWritable, Text>{
	
	private long start;
	private long end;
	private long index;
	private MultiFileSplit split;
	private int numPaths;
	
	public myRecordReader(MultiFileSplit Split)
    {
		this.end = Split.getLength(); //I think this is # chars in "path:0+size1" "path:0+size2" .. 
		this.start = 0;
		this.index=0;
		this.split = Split; //path:0+length  ... LATER should remove part of it to open the file
		this.numPaths = Split.getNumPaths();
		
		//split.getLocations();  <-- get the list of hostnames where the split is located
    }

	public boolean ColonExists(String s, int start){
		for(int i=start; i< (int)s.length();i++)
			if(s.charAt(i)==':') return true;
		return false;
	}
	public int getStartIndex(MultiFileSplit Split){
		return 1;
	}
	@SuppressWarnings("deprecation")
	public boolean next(LongWritable key,Text value){  //Next Record
		
		FSDataInputStream in = null;
		String path= null;
		int stop =0; 
		String line = new String();
		
		if(ColonExists(this.split.toString(),(int)this.index)){
			 stop = this.split.toString().indexOf(':');
			 path = this.split.toString().substring(0,stop);
		}
		/*
		try{
			FileSystem hdfs = FileSystem.get(wc.wcMultiFileInputFormat.uri,wc.WordCount.myconf);//new Configuration());//wc.WordCount.myconf);
			if(true) wc.wcMultiFileInputFormat.numFiles+=1000;
			in = hdfs.open(new Path(path));					
		}catch(Exception e) {wc.wcMultiFileInputFormat.numFiles+=100;}
		*/	
		if(this.index < this.end)
        {
			key.set(this.index); 
			/*
			try{
				line = in.readLine();// = in.readLine();
				//didn't work vec.add(in.readLine());// = in.readLine();
				//vec.add(in.readLine());
			  }catch(Exception e) {wc.wcMultiFileInputFormat.numFiles+=20;}
			*/
			//value.set(this.split.toString().substring(0,stop)); //Takes one file-path 
			//value.set(vec.elementAt(1));
			
			value.set(path);			
			//value.set(vec.get(0));
			this.index+=this.end;// this is ignored later
            return true;
        }
        else return false; /*
		if(this.index <= this.numPaths){
			key.set(index);
			value.set(split.getPath(0).getName());
		}
		return false;*/
	}	
	public void close(){}	
	public LongWritable createKey(){ return new LongWritable();	}
	public Text createValue(){	return new Text();	}
	public float getProgress(){
		if(this.index == this.end) return 0.0f;
		else 
			return Math.min(1.0f, (this.index-this.start)/(float)(this.end-this.start));
	}
	public long getPos() { 
		return this.end-this.index;
	}
	 	
}
