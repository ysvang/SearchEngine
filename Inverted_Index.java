import java.io.*;
import java.util.*;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.mongodb.*;
import org.jsoup.Jsoup;


public class Inverted_index{
	
	public static Map<String, WordPayload> invertedIndex = new HashMap<String, WordPayload>();
	public static Map<Integer, String> docID_to_url_map = new HashMap<Integer, String>();
	public static Set<String> stopWordsSet = new HashSet<String>();
	public static int docID_Iterator = 0;
	public static int totalNumberOfDoc = 0;
	
	// Helper method to parse the stop word file
	public static Set<String> stopWordsParser(String stopWordFile) {
		stopWordFile = stopWordFile.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
		String[] stopWordsTokens = stopWordFile.split("\\s+");	
		Set<String> stopWordsSet = new HashSet<String>(Arrays.asList(stopWordsTokens));
		return stopWordsSet;
	}	
	
	// Reads in stop word file and returns a collection set of stop words
	public static void processStopWordFile() {	
		// Load in stop word file
		File stopWordFile = new File("C:/Users/scott/workspace/stopwords.txt");
		try {	
			stopWordsSet = stopWordsParser(Files.toString(stopWordFile, Charsets.UTF_8));
		} catch (IOException e){
			System.err.println("Error reading in file: " + stopWordFile);
    		e.printStackTrace();
		}
	}
	
	// Parses each file and return an array of strings.
	// Tokens with length less than 2 or contained in set of stop words are set to null which is used later in n-gram processing.
	public static String[] parseText(String urlText){
		urlText = urlText.replaceAll("<[^>]*>", " ");
		//urlText = Jsoup.parse(urlText).text();
		//System.out.println(urlText);
		urlText = urlText.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
		String[] textFileTokens = urlText.split("\\s+");
		for(int i = 0; i < textFileTokens.length; i++){
			if (stopWordsSet.contains(textFileTokens[i]) || textFileTokens[i].length() < 2){
				textFileTokens[i] = null;
			}
		}
		return textFileTokens;
	}
	
	// Builds the inverted index for the crawled data
	public static void buildInvertedIndex(String[] textTokens, String url){
		docID_to_url_map.put(docID_Iterator, url);
		// find set of unique words so only have to do insertion once for each unique word
		Set<String> uniqueWords = new HashSet<String>(Arrays.asList(textTokens));
		for(String word : uniqueWords){
			if (word != null) {
				// tracks the position of each unique word in each document.  Size of list is term freq.
				List<Integer> wordPosInDoc = new ArrayList<Integer>();
				for(int i = 0; i < textTokens.length; i++){
					if ((textTokens[i] != null) && (textTokens[i].equals(word)) ){
						wordPosInDoc.add(i);
					}
				}
				if (invertedIndex.containsKey(word)) {
					WordPayload payload = invertedIndex.get(word);
					Documents doc = new Documents();
					doc.doc_id = docID_Iterator;
					doc.wordPos = wordPosInDoc;
					doc.tf = wordPosInDoc.size();
					payload.posting.add(doc);
					invertedIndex.put(word, payload);
				} else {
					WordPayload payload = new WordPayload();
					Documents doc = new Documents();
					doc.doc_id = docID_Iterator;
					doc.tf = wordPosInDoc.size();
					doc.wordPos = wordPosInDoc;
					payload.posting = new ArrayList<Documents>();
					payload.posting.add(doc);
					invertedIndex.put(word, payload);
				}
			}
		}
	}
	
	// iterates through each document of the DB to build inverted index
	public static void processDB(DBCursor cursor){
		DBObject doc;
		//int z = 0;
		while(cursor.hasNext()) {
			doc = cursor.next();
			docID_Iterator++;
			buildInvertedIndex(parseText(doc.get("text").toString()), doc.get("url").toString());
			docID_to_url_map.put(docID_Iterator, doc.get("url").toString());
			
			/*
			z++;
			if (z > 20) {
				break;
			}
			*/
		}
	}
	
	// Uses the variation of tf-idf shown in lecture and calculate tf-idf for each document of each term
	public static void tf_IDFNormalization(){
		Iterator<Map.Entry<String, WordPayload>> i = invertedIndex.entrySet().iterator(); 
		while(i.hasNext()){
		    String key = i.next().getKey();
		    WordPayload payload = invertedIndex.get(key);
		    List<Documents> posting = payload.posting;
		    for (Documents doc : posting){
		    	doc.tfidf = (1+Math.log10(doc.tf))* Math.log10(totalNumberOfDoc / posting.size());
		    }
		    payload.posting = posting;
		    invertedIndex.put(key, payload);
		}
	}
	
	
	
	// prints out the invertedIndex for visualization
	public static void printIndex(){
		Iterator<Map.Entry<String, WordPayload>> i = invertedIndex.entrySet().iterator(); 
		while(i.hasNext()){
		    String key = i.next().getKey();
		    WordPayload payload = invertedIndex.get(key);
		    List<Documents> posting = payload.posting;
		    System.out.println("word: " + key);
		    for (Documents doc : posting) {
		    	System.out.println("  documentID:" + doc.doc_id);
		    	System.out.println("    tf: " + doc.tf);
		    	System.out.println("    tf-idf: " + doc.tfidf);
		    	System.out.println("     Word position: ");
		    	for (Integer pos : doc.wordPos){
		    		System.out.println("       " + pos);
		    	}
		    }
		}
	}
	
	
	
	// prints out the docId to URL map for visualization
	public static void printDocID2URL(){
		Iterator<Map.Entry<Integer, String>> i = docID_to_url_map.entrySet().iterator(); 
		while(i.hasNext()){
		    Integer key = i.next().getKey();
		    String url = docID_to_url_map.get(key);
		    System.out.println("docID: " + key + ", url: " + url);
		}
	}
	
	
	// Save the inverted index to file via serialization
	public static void saveInvertedIndexToDisk(){
        try{
        	FileOutputStream fos = new FileOutputStream("C:/CS221/invertedIndex6.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(invertedIndex);
            oos.close();
            fos.close();
            System.out.println("Serialized HashMap data is saved in invertedIndex.ser");
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
	}
	
	// Save doc-to-URL Map to file via serialization
	public static void saveDocID2URLToDisk(){
        try{
        	FileOutputStream fos = new FileOutputStream("C:/CS221/docID2URL6.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(docID_to_url_map);
            oos.close();
            fos.close();
            System.out.println("Serialized HashMap data is saved in docID2URL.ser");
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
	}
	
	public static void main(String[] args) throws Exception {
		
		// Initialize mongoDB client for communication
		MongoClient mongoClient = new MongoClient();
		DB db = mongoClient.getDB("ics_db2");
		DBCollection table = db.getCollection("webpages2");
		DBCursor cursor = table.find();
		totalNumberOfDoc = (int) table.count();
		
		processStopWordFile();
		processDB(cursor);
		tf_IDFNormalization();
		//printIndex();
		//printDocID2URL();
		saveInvertedIndexToDisk();
		saveDocID2URLToDisk();
		System.out.println("The total number of document is: " + totalNumberOfDoc);
		System.out.println("The total number of unique words is: " + invertedIndex.size());
		mongoClient.close();
	}

}

// Class of payload value for each word in inverted index
class WordPayload implements Serializable{
	/**
	 * serialVersionUID used for serialization
	 */
	private static final long serialVersionUID = 1L;
	public List<Documents> posting;
}

// Class of document used to build up posting for each word in inverted index
class Documents implements Serializable{
	
	/**
	 * serialVersionUID used for serialization
	 */
	private static final long serialVersionUID = 1L;
	public int doc_id;
	public int tf;
	public double tfidf;
	public List<Integer> wordPos;
}