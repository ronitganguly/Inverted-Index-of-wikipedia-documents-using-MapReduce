

import utils.PorterStemmer;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.StringTokenizer;
//import java.util.Set;


//taken from the sample skeleton provided Prof. Nikos Ntarmos, The University Of Glasgow.

public class MapperVersionFour extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable > {
	static enum COUNTERS_ { DOC_LENGTH, NUM_DOCS};
	public CompositeKeyWritable frequencypair = new CompositeKeyWritable();
	
	public String composite_key;
	
	HashSet<String> stopwords = new HashSet<String>();
	
	PorterStemmer porterStemmer = new PorterStemmer();
	
	public NullWritable nullValue = NullWritable.get();
	String docid;
	//https://www.geeksforgeeks.org/distributed-cache-in-hadoop-mapreduce/
	public void setup(Context context) throws IOException {
		
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles != null && cacheFiles.length > 0) {
			try {
				String line;
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path getFilePath = new Path(cacheFiles[0].toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
				while ((line = reader.readLine()) != null) {
					this.stopwords.add(line);
					}
		}

			catch (Exception e) {
				System.out.println("File not found");
				System.exit(1);
			}
		}
	}
	
	public boolean stopWordChecking(String word) throws IOException {

		return stopwords.contains(word);
	}
	
	public String porterStemming(String term) {
		
		return this.porterStemmer.stem(term);
	}
	protected void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException 
	{
		 Hashtable<String,Integer> token_frequency_table = new Hashtable<String,Integer>();
		 String next_token;
		 Enumeration<String>  token_names;
		 
		 String doc = value.toString();
		 int doc_length =0;
		 //setup(context);                                     //this creates an ArrayList with all the stopwords
		 
		 String[] punct = {",",".","?","!","=",")","(","[","]","{","}",":",";"}; 
	    	
	    	
	    for (String each_punct: punct)
	    	doc= doc.replace(each_punct, "");        //removing punctuations (, . ? !)
	    
	    Scanner read_file = new Scanner(doc);
		 
		 String title_line = read_file.nextLine();
		 this.docid = title_line.replace("[[", "").replace("]]", "").replace(" ", "||");            //get the docid within [[....]]
	    
		 while(read_file.hasNextLine()) {
			 
			 String next_line = read_file.nextLine();
			 StringTokenizer tokenizer = new StringTokenizer(next_line);
			 while (tokenizer.hasMoreTokens()) {
				 
				 String token=tokenizer.nextToken(); 
				 if (!stopWordChecking(token.toLowerCase())) {           //if not a stop word then only proceed
					doc_length+=1;
					String stemmed_token = porterStemming(token);  //Applying stemming
					
					if (token_frequency_table.containsKey(stemmed_token))
						token_frequency_table.put(stemmed_token, token_frequency_table.get(stemmed_token)+1); //increment frequency
					else 
						token_frequency_table.put(stemmed_token,1);
						}
			 }
		 }		    
		    token_names = token_frequency_table.keys();
		      //iterate through hashtable

		      while(token_names.hasMoreElements()) {
		    	 next_token = (String) token_names.nextElement();

		    	 String token_frequency = token_frequency_table.get(next_token).toString();
		    	 this.composite_key = next_token+" "+this.docid+" "+token_frequency;
		    	 this.frequencypair.setComposite(composite_key);
		    	 context.write(frequencypair, nullValue);

		      }  

		      context.getCounter(COUNTERS_.DOC_LENGTH).increment(doc_length);  //global counter tracking doc length
		      
		      this.composite_key = "|@|"+this.docid+" "+Integer.toString(doc_length);  //tagging key with @ when it is docId - doclength
		      
		  
		      this.frequencypair.setComposite(composite_key);
		      //context.write(this._docid, this.docid_length_frequency );  //old version
		      
		     context.write(frequencypair, nullValue);  //write the docid-doclength

		      context.getCounter(COUNTERS_.NUM_DOCS).increment(1);  //global counter tracking total docs read
	 
		      read_file.close();           //close the file
	}
}
