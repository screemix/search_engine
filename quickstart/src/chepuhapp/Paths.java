package chepuhapp;

public class Paths {
    public static String INPUT = "data/input";
    
    public static String OUTPUT_TF = "data/output_tf";
    public static String OUTPUT_IDF = "data/output_idf";
    
    public static String INPUT_INDEXER_TF = OUTPUT_TF + "/part-r-00000";
    public static String INPUT_INDEXER_IDF = OUTPUT_IDF + "/part-r-00000";
    public static String OUTPUT_INDEXER = "data/output_indexer";
    
    public static String INPUT_DOC_VECTORIZER = OUTPUT_INDEXER + "/part-r-00000";
    public static String OUTPUT_DOC_VECTORIZER = "data/output_vectorizer";
    
    public static String INPUT_RELEVANCE = OUTPUT_DOC_VECTORIZER + "/part-r-00000";
    public static String OUTPUT_RELEVANCE = "data/output_relevance";
    
    public static String OUTPUT_QUERY = "data/output_query";
    
    public static String INPUT_EXTRACTOR = OUTPUT_RELEVANCE + "/part-r-00000";
    public static String OUTPUT_EXTRACTOR = "data/output_extractor";
    
    public static String INPUT_TOPN = OUTPUT_EXTRACTOR + "/part-r-00000";
    public static String OUTPUT_TOPN = "data/output_topn";

}
