package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataOutput;
import java.io.DataInput;
import java.lang.Integer;

public class intermediate implements Writable{ //otherwise get serialization error
//public Text file;
//public IntWritable score;	   
	   // private String file;
	    public int score;
	    public String file;
	    
	  intermediate(){this.score=0;}
	  intermediate(int x,String s){
	  //intermediate(int x){		  
		  this.score = x;
		  this.file = s;
	  }
	  public void write(DataOutput out) {		  
    	  try{    		
    		  out.writeInt(this.score);
    		  out.writeChars(this.file);
    	}catch(Exception e){;}
      }

      public void readFields(DataInput in) {
       try{    	   
    	   this.score = in.readInt();
    	   this.file = in.readLine();
       }catch(Exception e){;}
      }
      
      //The following is needed to see the output of map, otherwise unknown characters..
      public String toString(){ 
    	  return ('<'+this.file+','+Integer.toString(this.score)+'>');
      }
      
      public int getScore(){ return this.score;}
      public String getFile() {return this.file;}
     // public String getFile() {return this.file;}
     // public int getScore() {return this.score;}
      
	  /*
	  intermediate(String s, int x){
		  file = new Text(s);
		  score = new IntWritable(x);
	  }
	  
	  intermediate(Text t, IntWritable i){
		  file = t;
		  score = i;
	  }

	  public void write(DataOutput out) {
		  String str= file.toString();
		  int i= score.get();
    	  try{
    		  out.writeChars(str);
    		  out.writeInt(i);
    	}catch(Exception e){;}
      }      
      public void readFields(DataInput in) {
		  String str=null;
		  int i=0;
       try{
    	   str = in.readLine();
    	   i = in.readInt();    	   
       }catch(Exception e){;}
		  file = new Text(str);
		  score = new IntWritable(i);
      }
      
      public static intermediate read(DataInput in){
        intermediate w = new intermediate();
        w.readFields(in);
        return w;
      }*/
}