# Inverted-Index-of-wikipedia-documents-using-MapReduce

Wikipedia files of size over 19GB has been parsed and inverted index has been created.

Our main task is to implement a watered-down version of a full-text search engine. The end result
should be able to receive a set of keywords and produce a list of the top-10 best matches from the
Wikipedia articles in the dataset in decreasing score order, along with the matching score. To
produce these lists, we will need to first construct an “index” that will allow us to compute a score
for each article. The score of each article will be computed using the Okapi BM25 ranking function.

Specifically, given a query Q containing n terms t1, t2, …, tn, the score of a document (article) d in a
dataset D will be given by:
scoreBM25(Q, d) = Σt  Q[IDF(t) * (2.5 * ft,d) / (ft,d + 1.5 * (0.25 + 0.75 * |d| / avgdl))]
where IDF(t) = log((N - nt + 0.5) / (nt + 0.5)), nt is the number of documents in D containing the term
t, N is the number of all documents in D, ft,d is the number of occurrences of term t in document d

1 Source: http://kopiwiki.dsd.sztaki.hu/
2 https://en.wikipedia.org/wiki/Okapi_BM25 
(a.k.a., term frequency), |d| is the length of document d in number of words/terms, and avgdl is the
average length across all documents in the dataset.
Not all terms in a document should be considered when computing the above metrics; specifically,
so-called stopwords should be ignored. Moreover, to avoid inflected or derived words affecting the
metrics, terms should be stemmed. Specifically, given a document (article), we should first remove
all stopwords (a short list of such terms for the English language will be included under
src/main/resources, borrowed from the source code of the Terrier open source search engine3,4),
then stem remaining words (the implementation of a stemmer for the English language – specifically
the Porter Stemmer5 will be provided), then proceed to compute the term frequency, IDF, and utterly the BM25 score.

In the second part of this coursework we will be asked to implement the above using either
Hadoop/MapReduce or Spark.  Based on this knowledge, we should now design two
main components: (a) an “indexer” that precomputes and stores as much of the above information
as we deem necessary, and (b) a query processor that uses said information to produce the list of
top-10 best matches to arbitrary user-supplied queries. 

### For further details, please read the https://github.com/ronitganguly/Inverted-Index-of-wikipedia-documents-using-MapReduce/blob/master/Inverted%20Index%20Report.pdf

### Important MapReduce concepts learned:
1. Distributed cache: In this project, we are removing the stop words before indexing. So, a stop word file is provided to all the mappers.
2. Value tagging: Since Mapper releases two types of outputs in this project, the reducer, comparator, partitioner have to know which kind of key they are facing currently. So, a special value is tagged to one kind of key in order to differentiate.
3. Custom writable class: This class also implements a compareTo method which compares the tokens from each other and then it compares the frequency of the token within those tokens(property of inverted indexes).
4. Secondary sorting on composite keys.
5. Multiple outputs: Two files are being produced at the end. 


## Brief discussion on the plan of attack:

### Mapper:

1) The wikipedia file has several documents with their title, and looks like:

\[\[Title]]

passages...........

..............

.........

2) Record reader is going to split on "\n\[\[". So, at a time, a mapper will have one document along with its title.

3) Input format of mappers is TextInputFormat ie LongWritable and Text. Where Longwritable is the offset of the split and Text is the entire document along with the title.

4) Using distributed cache we make the stop word file available to every mapper. 

5) Mapper process includes a) removing of stopwords in setup method. b) Stemming using Porter stemmer. c) releasing outputs.

6) Here, there are two types of outputs being emitted by the mapper: a) <CompositeKeyWritable, NullWritable> : <token title frequency, Null> b) <CompositeKeyWritable, NullWritable> : <docid doclength, Null>

7) Two types of key-value pairs require some sort of value tagging to differentiate. So, second type of K-V has "|@|" value tagged in front.

8) Also, two global counters are being maintained: doc length(total tokens) and total documents(which is 1 per mapper)

### CompositeKeyWritable:

It implements compareTo() method which allows *CustomGroupComparator* to compare the patitioned mapper outputs(only the first kind of k-v pair) like this:
The composite key is : token docid frequency, so firstly, token is sorted then within each token, pretending docid and frequency as a single entity, frequecies are sorted.
ex- unsorted: 'Apple' 'docid_1' 33
              'Guava' 'docid_2' 10
              'Apple' 'docid_9' 21
              
    sorted :  'Apple' 'docid_9' 21
              'Apple' 'docid_1' 33
              'Guava' 'docid_2' 10
              
 ### CustomPartitioner:
 
 It simply makes sure that no matter what kind of key-value pair is this, all the K-V pair having the same token (if first kind of K-V pair) or having the same title (if second kind of K-V pair) get sent to the same reducer.
 
 ### Reducer:
 
 It does nothing but just writes into two types of files using multiple outputs in mapreduce concept. So, it simply gets the two types of outputs from the mapper and simply writes them in two different files.
