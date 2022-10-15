package edu.upenn.cis.nets2120.hw1;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class QueryForWord {
	/**
	 * A logger is useful for writing different types of messages
	 * that can help with debugging and monitoring activity.  You create
	 * it and give it the associated class as a parameter -- so in the
	 * config file one can adjust what messages are sent for this class. 
	 */
	static Logger logger = LogManager.getLogger(QueryForWord.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	
	/**
	 * Inverted index
	 */
	Table iindex;
	
	Stemmer stemmer;

	/**
	 * Default loader path
	 */
	public QueryForWord() {
		stemmer = new PorterStemmer();
	}
	
	/**
	 * Initialize the database connection
	 * 
	 * @throws IOException
	 */
	public void initialize() throws IOException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		logger.debug("Connected!");
		iindex = db.getTable("inverted");
	}
	
	boolean isWord(String s) {
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))) { //if it isn't in A-Z or a-z
				return false;
			}
		}
		return true;
	}
	
	boolean isStopWord(String s) {
		String str = s.toLowerCase();
		if (str == "a" || str == "all" || str == "any" || str == "but" || str == "the") {
			return true;
		}
		return false;
	}
	
	public Set<Set<String>> query(final String[] words) throws IOException, DynamoDbException, InterruptedException {
		Set<Set<String>> res = new HashSet<>(); // what we will return
		
		for (int i = 0; i < words.length; i++) { //truncate all the words in the array if it is valid
			String s = words[i];
			String sToLower = s.toLowerCase();
			if (isWord(sToLower) || (!(isStopWord(sToLower)))) {
				words[i] = (String) stemmer.stem(sToLower);
			} else {
				words[i] = null;
			}
		}
		for (String w : words) { //for all of the words in the array
			HashSet<String> urls = new HashSet<>();
			if (w != null) { //get all of the corresponding urls for the word
				ItemCollection<QueryOutcome> allItems = iindex.query("keyword", w);
				for (Item item : allItems) {
					urls.add(item.getJSON("url"));
				}
			}
			res.add(urls);
			
		}
		return res;
		
//		failed attempt
//		PorterStemmer ps = new PorterStemmer();
//		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
//		Set<Set<String>> res = new HashSet<Set<String>>(); //what we are going to output
//		for (int i = 0; i < words.length; i++) {
//			String currentWord = words[i];
//			HashSet<String> currentSet = new HashSet<String>();
//			if (!isWord(currentWord) || isStopWord(currentWord)) {
//				res.add(currentSet);
//			} else {
//				String word = (String) stemmer.stem(currentWord.toLowerCase());
//				QuerySpec spec = new QuerySpec().withKeyConditionExpression(word);
//				ItemCollection<QueryOutcome> items = iindex.query(spec);
//				Iterator<Item> iterator = items.iterator();
//				Item item = null;
//				while (iterator.hasNext()) {
//					item = iterator.next();
//					currentSet.add(item.getString("url"));
//				}
//				res.add(currentSet);
//			}
//		}
//		return res;
	}

	/**
	 * Graceful shutdown of the DynamoDB connection
	 */
	public void shutdown() {
		logger.info("Shutting down");
		DynamoConnector.shutdown();
	}

	public static void main(final String[] args) {
		final QueryForWord qw = new QueryForWord();

		try {
			qw.initialize();

			final Set<Set<String>> results = qw.query(args);
			for (Set<String> s : results) {
				System.out.println("=== Set");
				for (String url : s)
				  System.out.println(" * " + url);
			}
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			qw.shutdown();
		}
	}

}
