# Search Engine with Hadoop MapReduce
Project for introduction to Big Data Course, F20 
Alla Chepurova, Ruslan Mihailov

## 1. Task description 
### Motivation and goals of project
Nowadays developing systems of search engines goes hand in hand with big data applications. The big data area has expanded the opportunities for developing new search frameworks that are composed of reliable and more optimized analytics.

The goal for this project was to enhance our skill of writing MapReduce tasks in Java by implementing a naive search engine for finding relevant information corresponding to query that user makes among documents, which are placed in HDFS.
## 2. Search engine description

![](https://i.imgur.com/kDhm1ns.png)


### Indexer
**Indexer** is a logical unit of our project which job results in a set of processed input documents that later will be used by **Querier**. Here are the main components of it:

* **TFCounter** - goes through all the input documents and outputs the amount of times a specific word occurs in each document in format [(word, doc_id), 1].
*  **IDFCounter** - goes through all the input documents and outputs the number many documents a unique word occurs in in format (word, IDF).
*  **TFIDFCounter** - goes through the output of **TFCounter** and normalizes the TF number dividing it by the corresponding IDF number of the word. Output looks like [doc_id, (word, TF/IDF)].
*   **DocumentVectorizer** - goes through the unordered output of **TFIDFCounter** and creates a structured representation of the each document by its id. After all we will get file with (doc_id, {word1: TF/IDF1, word2: TF/IDF2, ...}).

### Querier
**Querier** is another logical unit in our Search Engine. It processes the query from the CLI and consists of below units:
* **QueryVectorizer** - component which processes the query string and returns its vectorized form. It looks like something like this: "word1 word2 word3" -> {"word1":"idf1", "word2":"idf2", "word3":"idf3"}
* **RankingVectorizer** - returns sorted list of all documents with their rankings with respect to the query in format (doc_id, rank). It implements the following formula between query words and vocabulary to find the ranking score of document
![](https://i.imgur.com/eVv72dw.png)
* **TitleExtractor** - component which finds corresponding titles to the elected documents by their id so the overall output looks like this: (doc_id, doc_title, doc_rank)
* **SaveFirstN** - depending on number of N returns and writes on disk first N documents that are relevant to the query in format (doc_id, rank)

## 3. How to launch

The main execuable file is called ”final.jar”. The next commands should be used to launch the corresponding services:

Indexer:
```hadoop jar path/to/final.jar chepuhapp.Main Index /path/to/input```

Querier:
```hadoop jar path/to/final.jar chepuhapp.Main Query N "Query text" ```

## 4. Outcomes
The team has achieved the project goal, and developed naive search engine.
Moreover, as a learning result of accomplishing of the project, all team members:
* Practiced competencies of:
    * 1 Java programming
    * 2 Setting up HDFS
    * 3 Java files compiling, setting up IDEs, Maven configuring
    * 4 Git branching
* Acquired knowledge of the following areas:
    * 1 MapReduce concepts
    * 2 Basics of Information Rertivial
    * 3 Java OOP
    * 4 Search engines concepts

## 5. Sourses
1. [Big Data Analytics for Search Engine Optimization](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwihheiptYLsAhVPAxAIHcVgAG4QFjAKegQIBRAB&url=https%3A%2F%2Fwww.mdpi.com%2F2504-2289%2F4%2F2%2F5%2Fpdf&usg=AOvVaw2JMMShJkpOsauamClDcuwz)
2. [MapReduce Tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
