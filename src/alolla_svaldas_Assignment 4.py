from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import re

# Step 1: split and normalize
def normalize_and_split(line):
    return re.sub(r'[^\w\s]', '', line).lower().split()

# Step 2; Generate Bigrams
def generate_bigrams(words_list):
    return zip(words_list, words_list[1:])


try:
    spark_context = SparkContext.getOrCreate()  
    spark_context.stop()
except ValueError:
    pass
spark_context = SparkContext("local[2]", "NetworkBigramCount")  


stream_context = StreamingContext(spark_context, 1)

#Step3:  Create a DStream that connects to localhost:9999
line_stream = stream_context.socketTextStream("localhost", 9999)

open('result.txt', 'w').close()

# Split each line into words, normalize to lowercase and remove punctuation
normalized_words = line_stream.flatMap(normalize_and_split)

# Create bigrams from the words
bigrams_stream = normalized_words.mapPartitions(lambda iter: generate_bigrams(list(iter)))

# Define the window length and sliding interval
window_length = 3  
sliding_interval = 2  

# Apply the window function and count each bigram
windowed_bigrams = bigrams_stream.window(window_length, sliding_interval)
bigram_counts = windowed_bigrams.map(lambda bigram: (bigram, 1)).reduceByKey(lambda x, y: x + y)

def print_results(rdd):
    if not rdd.isEmpty():
        with open('result.txt', 'a') as result_file:
            for line in rdd.collect():
                result_file.write(str(line) + '\n')

bigram_counts.foreachRDD(print_results)  

# Step4: streaming computation
stream_context.start()

# Wait for the streaming to finish
stream_context.awaitTermination()
