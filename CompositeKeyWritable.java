
//https://javadeveloperzone.com/hadoop/hadoop-create-custom-value-writable-example/
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeKeyWritable  implements Writable, WritableComparable<CompositeKeyWritable>{
	
	
	public Text composite_key;
	
	public CompositeKeyWritable() {
        
		composite_key = new Text();
    }

	public CompositeKeyWritable(String compositekey) {
		composite_key = new Text(compositekey);
	}
	
	 public void write(DataOutput dataOutput) throws IOException {
		 	
		 composite_key.write(dataOutput);
	    }

	 public void readFields(DataInput dataInput) throws IOException {
		 	
		 composite_key.readFields(dataInput);
	    }
	 
	
	 
	 public String toString() {
	        
	        return getComposite().toString();
	               
	 	}

	 
	 public void setComposite(String compositekey) {
		 composite_key = new Text(compositekey);
		 }
	 
	 public Text getComposite() {
		 return this.composite_key;
	 }
	 
	 public String getNaturalKey() {
		 String[] term_string =this.composite_key.toString().split(" ");
		 return term_string[0];
	 }
	 
	@Override
	public int compareTo(CompositeKeyWritable frequencypair) {
		
		String composite_key_string =frequencypair.composite_key.toString();
		
		if (!composite_key_string.startsWith("|@|")) {   						//value tagging and sorting posting list
		
			String[] record1 = composite_key_string.split(" ");
			String[] record0 = this.composite_key.toString().split(" ");
		
			int c_value = record0[0].compareTo(record1[0]);
			if (c_value ==0)                                 					  //if the terms are the same
			{
				if (Integer.parseInt(record0[2]) < Integer.parseInt(record1[2]))  //index 2 is term frequency
					c_value = -1;
				else if (Integer.parseInt(record0[2]) == Integer.parseInt(record1[2]))
					c_value = 0;
				else
					c_value = 1;
				return -1*c_value;          									// sort descending within the terms i.e. sort the frequencies in descending order
			}
			else
				return c_value;            										// sort the outer terms in ascending 
		}
		
		else          															//sorting docid-doclength key value type
		{
			String[] record1 = composite_key_string.split(" ");
			String[] record0 = this.composite_key.toString().split(" ");
			
			int c_value = record0[0].compareTo(record1[0]);
			return c_value;                                   					//sort ascending i.e sort by docid
		}
		
	}
}
