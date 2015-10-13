/* This program asks a user for a query and then returns the search result at the command line interface.
 * Author: Scott Vang
 */

import java.io.*;
import java.util.*;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.mongodb.*;


public class SearchQuery {
	
	public static Map<String, WordPayload> invertedIndex = new HashMap<String, WordPayload>();
	public static Map<Integer, String> docID_to_url_map = new HashMap<Integer, String>();
	public static Set<String> stopWordsSet = new HashSet<String>();
	// database objects
	public static MongoClient mongoClient;
	public static DB db;
	public static DBCollection table;
	public static DBCursor cursor;
	
	// Reads back in the perviously saved inverted index
	public static void deserializeInvertedIndex(){
		try{
			FileInputStream fis = new FileInputStream("C:/CS221/invertedIndex6.ser");
		    ObjectInputStream ois = new ObjectInputStream(fis);
		    @SuppressWarnings("unchecked")
		    Map<String, WordPayload> map = (Map<String, WordPayload>) ois.readObject();
		    ois.close();
		    fis.close();
		    invertedIndex = map;
		}catch(IOException ioe){
			ioe.printStackTrace();
			return;
		}catch(ClassNotFoundException c){
			System.out.println("Class not found");
		    c.printStackTrace();
		    return;
		}
	}	
	
	// Reads back in map of DocID to URL
	public static void deserializeDocID2URL(){
		try{
			FileInputStream fis = new FileInputStream("C:/CS221/docID2URL6.ser");
		    ObjectInputStream ois = new ObjectInputStream(fis);
		    @SuppressWarnings("unchecked")
		    HashMap<Integer, String> map = (HashMap<Integer, String>) ois.readObject();
		    ois.close();
		    fis.close();
		    docID_to_url_map = map;
		}catch(IOException ioe){
			ioe.printStackTrace();
			return;
		}catch(ClassNotFoundException c){
			System.out.println("Class not found");
		    c.printStackTrace();
		    return;
		}
	}

	// Print a few statistics of each URL (for testing)
	public static void printIndex(){
		Iterator<Map.Entry<String, WordPayload>> i = invertedIndex.entrySet().iterator(); 
		int j = 0;
		while(i.hasNext()){
		    String key = i.next().getKey();
		    WordPayload payload = invertedIndex.get(key);
		    List<Documents> posting = payload.posting;
		    System.out.println("word: " + key);
		    for (Documents doc : posting) {
		    	System.out.println("  documentID:" + doc.doc_id);
		    	System.out.println("     Word position: ");
		    	for (Integer pos : doc.wordPos){
		    		System.out.println("       " + pos);
		    	}
		    }
		    j++;
		    if (j >5){
		    	break;
		    }
		    
		}
	}
	
	
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
	
	// Tokenize each query
	public static List<String> parseQuery(String queryInput){
		String query_str = queryInput.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
		String[] query_Tokens = query_str.split("\\s+");
		List<String> queryTokens = new ArrayList<String>();
		for (int i = 0; i < query_Tokens.length; i++){
			if (invertedIndex.containsKey(query_Tokens[i])){
				queryTokens.add(query_Tokens[i]);
			}
		}
		return queryTokens;
	}
	
	
	// Prints out result
	public static void printResult(List<Integer> orderedResult){
		System.out.println("Here are your results: ");
		if (orderedResult.size() == 0){
			System.out.println("..No result for that query.. \n");
			return;
		}
		
		int j = 0;
		for(int i= 0; i < orderedResult.size(); i++){
			System.out.println(docID_to_url_map.get(orderedResult.get(i)));
			if (j == 10){
				System.out.println("\n");
				return;
			}
			j++;
		}
		System.out.println("\n");
		return;
	}
	
	// Parses each file and return an array of strings.
	// Tokens with length less than 2 or contained in set of stop words are set to null which is used later in n-gram processing.
	public static String[] parseText(String urlText){
		urlText = urlText.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
		String[] textFileTokens = urlText.split("\\s+");
		for(int i = 0; i < textFileTokens.length; i++){
			if (stopWordsSet.contains(textFileTokens[i]) || textFileTokens[i].length() < 2){
				textFileTokens[i] = null;
			}
		}
		return textFileTokens;
	}
	
	// Calculate similarity score for each relevant docs 
	public static List<Integer> simScoring(List<Documents> relevantDocs, List<String> queryTokens){
		List<Integer> resultList = new ArrayList<Integer>();
		
		// calculates cosine similarity
		Map<Integer, Double> cosineScore = new TreeMap<Integer, Double>();
		Set<Integer> docSet = new HashSet<Integer>();
		for (Documents doc : relevantDocs) {
			if (!docSet.contains(doc.doc_id)){
				docSet.add(doc.doc_id);
				String webpage = docID_to_url_map.get(doc.doc_id);
				BasicDBObject query = new BasicDBObject("url", webpage);
				cursor = table.find(query);
				DBObject dbDoc = new BasicDBObject();
				try {						
					while(cursor.hasNext()) {
						dbDoc = cursor.next();
					}
				} finally {
					cursor.close();
				}
				
				String webpageText = dbDoc.get("text").toString();
				String[] textTokens = parseText(webpageText);
				Map<String, Integer> docTF = new HashMap<String, Integer>();
				
				for (String token : textTokens){
					if (docTF.containsKey(token)){
						docTF.put(token, docTF.get(token)+1);
					} else {
						docTF.put(token, 1);
					}
				}
				double normalizeLength = 0.0;
				for (Map.Entry<String, Integer> entry : docTF.entrySet()){
					normalizeLength += (entry.getValue() * entry.getValue());
				}
				normalizeLength = Math.sqrt(normalizeLength);
				for (int i = 0; i < queryTokens.size(); i++){
					if (cosineScore.containsKey(doc.doc_id)){
						cosineScore.put(doc.doc_id, cosineScore.get(doc.doc_id) + docTF.get(queryTokens.get(i))/normalizeLength);
					} else {
						cosineScore.put(doc.doc_id, docTF.get(queryTokens.get(i))/normalizeLength);
					}
				}
			}
		}
		
		// Add tf-idf to each cosine score
		for (Documents d : relevantDocs){
			//System.out.println("cosine: " + cosineScore.get(d.doc_id));
			//System.out.println("tf: " + d.tfidf);
			cosineScore.put(d.doc_id, cosineScore.get(d.doc_id) + (d.tfidf*1/50));
		}
		
		MapSorter mapComparator =  new MapSorter(cosineScore);
        TreeMap<Integer, Double> cosineSorted = new TreeMap<Integer, Double>(mapComparator);
        cosineSorted.putAll(cosineScore);
        
        int k = 0;
        for (Map.Entry<Integer, Double> entry : cosineSorted.entrySet()){
        	resultList.add(entry.getKey());
        	if (k > 9){
        		break;
        	}
        	k++;
        }
		return resultList;
	}
	
	// gets result of searched query
	public static void getResult(List<String> queryTokens){
		List<Integer> orderedResult = new ArrayList<Integer>();
		boolean merge = true;
		List<Documents> relevantDocs = new ArrayList<Documents>();
		if (queryTokens.size() == 1){
			if(invertedIndex.get(queryTokens.get(0)) == null){
				// do nothing
			} else {
				WordPayload payload = invertedIndex.get(queryTokens.get(0));
				orderedResult = simScoring(payload.posting, queryTokens);
		    }
		} else {
			Map<Documents, Integer> docFreq = new HashMap<Documents, Integer>();
			Map<Integer, Integer> docID_Freq = new HashMap<Integer, Integer>();
			for (int i = 0; i < queryTokens.size(); i++){
				if (invertedIndex.containsKey(queryTokens.get(i))){
					WordPayload payload = invertedIndex.get(queryTokens.get(i));
					for (Documents d : payload.posting){
						if (docFreq.containsKey(d)){
							docFreq.put(d, docFreq.get(d)+1);
						} else {
							docFreq.put(d, 1);
						}
						
						if (docID_Freq.containsKey(d.doc_id)){
							docID_Freq.put(d.doc_id, docID_Freq.get(d.doc_id)+1);
						} else {
							docID_Freq.put(d.doc_id, 1);
						}
						
					}
				} else {
					merge = false;
				}
			}
			
			if (merge == true) {
				int i = 0;
				for (Map.Entry<Integer, Integer> entry : docID_Freq.entrySet()){
					if (entry.getValue() > 1) {
						for (Map.Entry<Documents, Integer> entry2 : docFreq.entrySet()){
							if (entry2.getKey().doc_id == entry.getKey()){
								relevantDocs.add(entry2.getKey());
								i++;
								if (i > 20){
									break;
								}
							}
						}
					}
				}
			} else {
				for (Map.Entry<Documents, Integer> entry : docFreq.entrySet()){
					relevantDocs.add(entry.getKey());
				}
			}
			orderedResult = simScoring(relevantDocs, queryTokens);
		}
		printResult(orderedResult);	
	}
	
	public static void main(String[] args) throws Exception {
		mongoClient = new MongoClient();
		db = mongoClient.getDB("ics_db2");
		table = db.getCollection("webpages2");
		//cursor = table.find();
		
		System.out.println("Deserializing inverted index...");
		deserializeDocID2URL();
		deserializeInvertedIndex();
	    System.out.println("...completed");
	    System.out.println(invertedIndex.size());
	    System.out.println(docID_to_url_map.size());
	    //printIndex();
	    
        Scanner scanner = new Scanner (System.in);
        String userQuery;
        processStopWordFile();
	    while(true){
	    	
	    	System.out.println("Enter a query here: ");
	    	userQuery = scanner.nextLine();
	    		    	
	    	if (userQuery.equals("quit")){
	    			break;
	    	} else {
		    	getResult(parseQuery(userQuery));	
	    	}
	    }
	    scanner.close();
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

// Method to help sort
class MapSorter implements Comparator<Integer> {

    Map<Integer, Double> pair;
    public MapSorter(Map<Integer, Double> unsortedMap) {
        this.pair = unsortedMap;
    }
  
    public int compare(Integer a, Integer b) {
        if (pair.get(a) >= pair.get(b)) {
            return -1;
        } else {
            return 1;
        } 
    }
}
