
import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



//some functions taken from the provided skeleton codes by the University of Glasgow.

public class CustomReducerClass extends Reducer<CompositeKeyWritable, NullWritable, Text, NullWritable> {
	
	private MultipleOutputs<Text, Writable> posting_list;
	private MultipleOutputs<Text, Writable> document_list;
	
	
	protected void setup(Context context) {            //https://gist.github.com/need4spd/4584416
		//muloutput = new MultipleOutputs(context);
		posting_list = new MultipleOutputs(context);
		document_list = new MultipleOutputs(context);
	}
	
	protected void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
		
		//for (DocidFrequencyWritable value:values) {
		
		Text reducerKey = key.getComposite();
		
		if (reducerKey.toString().startsWith("|@|")) //docid-doclength
		{
			//write to docid-doclength file
			Text natural_key = new Text();
			natural_key.set(reducerKey.toString().replace("|@|", "").replace("||", " "));  // remove the value tag and docID word separator (||)
			document_list.write("DocumentLength", natural_key, NullWritable.get());
		}
		
		else
		{
			Text natural_key = new Text();
			natural_key.set(reducerKey.toString().replace("||", " "));   //replace the docID word separator
			posting_list.write("PostingList", natural_key, NullWritable.get());
			//write to posting list file
		
		}
		
	}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			posting_list.close();
			document_list.close();
			 
		
		}
		
		
	
}
