package wc;

import java.util.Comparator;

/* THE COMPARATOR WILL KEEP QUEUE ORDERED SUCH THAT FIRST ELEMENT HAS MIN SCORE */
public class myComparator implements Comparator<intermediate>{
	  public int compare(intermediate o1, intermediate o2){
		  
		  //assert (o1.score == -1*o2.score); 		  
		  if(o1.getScore() == o2.getScore()) return 0;
		  else if ((o1.getScore() < o2.getScore())) return -1;
		  else return 1;
	  }
}