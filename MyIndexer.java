
import java.net.URI;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MyIndexer extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration myconf = getConf();
		myconf.set("textinputformat.record.delimiter","\n[[");  //delimiter for the record reader
		
		//myconf.set("mapreduce.framework.name", "local");
        //myconf.set("fs.defaultFS", "file:///");
		
		
		FileSystem fs = FileSystem.get(myconf);
		fs.copyFromLocalFile(new Path("file:///users/pgt/2487190g/uog-bigdata/src/main/resources/stopword-list.txt"), 
				new Path("hdfs://ideasup.dcs.gla.ac.uk:8020/user/2487190g/stopword-list.txt"));
		
		Job job = Job.getInstance(myconf, "Inverted Index");
		job.addCacheFile(new URI("hdfs://ideasup.dcs.gla.ac.uk:8020/user/2487190g/stopword-list.txt"));
		job.setJobName("InvertedIndex Job");
		job.setJarByClass(MyIndexer.class);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MapperVersionFour.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(CustomReducerClass.class);
		
		job.setNumReduceTasks(210);
		
		job.setGroupingComparatorClass(CustomGroupingComparator.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "DocumentLength", TextOutputFormat.class,
				CustomPartitioner.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "PostingList", TextOutputFormat.class,
				CustomPartitioner.class, NullWritable.class);
		
		
		
		int returnValue = job.waitForCompletion(true) ? 0:1; 
		
		if (job.isSuccessful()){
			Counters counters = job.getCounters();
			Counter doc_num = counters.findCounter(MapperVersionFour.COUNTERS_.NUM_DOCS);  //total documents read
        
        
			Counter total_doc_length = counters.findCounter(MapperVersionFour.COUNTERS_.DOC_LENGTH);  //total length of all the documents combined.
			double avg_doc = (double)total_doc_length.getValue()/ (double)doc_num.getValue();
        
			DecimalFormat d_format = new DecimalFormat("#.###");                           //formating to 3 decimal places
			System.out.println("The Job was successful");
			System.out.println("The average document length: "+ d_format.format(avg_doc)); //Average document length
		}
		
		else
			System.out.println("The Job was unsuccessful");
		
        return returnValue;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MyIndexer(), args));
	
	}
}