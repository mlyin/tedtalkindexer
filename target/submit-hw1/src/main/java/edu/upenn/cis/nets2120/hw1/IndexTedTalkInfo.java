package edu.upenn.cis.nets2120.hw1;

import java.util.Arrays;
import java.util.*;
import java.util.HashSet;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import edu.upenn.cis.nets2120.hw1.files.TedTalkParser.TalkDescriptionHandler;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * Callback handler for talk descriptions.  Parses, breaks words up, and
 * puts them into DynamoDB.
 * 
 * @author zives
 *
 */
public class IndexTedTalkInfo implements TalkDescriptionHandler {
	static Logger logger = LogManager.getLogger(TalkDescriptionHandler.class);

  final static String tableName = "inverted";
	int row = 0;
	
	SimpleTokenizer model;
	Stemmer stemmer;
	DynamoDB db;
	Table iindex;
	
	public IndexTedTalkInfo(final DynamoDB db) throws DynamoDbException, InterruptedException {
		model = SimpleTokenizer.INSTANCE;
		stemmer = new PorterStemmer();
		this.db = db;

		initializeTables();
	}

	/**
	 * Called every time a line is read from the input file. Breaks into keywords
	 * and indexes them.
	 * 
	 * @param csvRow      Row from the CSV file
	 * @param columnNames Parallel array with the names of the table's columns
	 */
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

	@Override
	public void accept(final String[] csvRow, final String[] columnNames) {
		PorterStemmer ps = new PorterStemmer(); //new stemmer
		Collection<Item> batchitems = new HashSet<Item>(); //collection of batch items for further putting into the table
		String id = lookup(csvRow, columnNames, "talk_id"); //lookup for id and url
		String url = lookup(csvRow, columnNames, "url"); //look up for url
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		
		for (int i = 0; i < csvRow.length; i++) { //initially iterate through all of the csv rows
			if (columnNames[i].equals("title") || columnNames[i].equals("all_speaker") || columnNames[i].equals("occupations")
					|| columnNames[i].equals("about_speakers") || columnNames[i].equals("topics") || columnNames[i].equals("description")
					 || columnNames[i].equals("transcript") || columnNames[i].equals("related_talks")) {
				String[] tokenizedColumn = model.tokenize(csvRow[i]); //tokenize each row
				for (int j = 0; j < tokenizedColumn.length; j++) { //for each one of the words in the tokenized row, 
					if (isWord(tokenizedColumn[j].toLowerCase()) && (!isStopWord(tokenizedColumn[j].toLowerCase()))) { //check if it satisfies conditions
						String stemmedWord = ((String) stemmer.stem(tokenizedColumn[j])).toLowerCase(); //if it does satisfy conditions, we stem it
						batchitems.add(new Item().withPrimaryKey("keyword", stemmedWord, "inxid", Integer.parseInt(id)).withString("url", url)); //add to current batch
						System.out.println(stemmedWord);
						if (batchitems.size() == 25) { //process in batches of 25
							System.out.println("BATCHPROCESSED"); //for debugging
							TableWriteItems tbl = new TableWriteItems("inverted").withItemsToPut(batchitems); //write to database
							BatchWriteItemOutcome outcome = db.batchWriteItem(tbl); //write to database
							batchitems = new HashSet<Item>(); //reset batchitems collection
						}
					}
				}
			}
		}
		if (batchitems.size() == 0) {
			System.out.println("Cnt equals 0, terminate");
			return;
		}
		TableWriteItems tbl = new TableWriteItems("inverted").withItemsToPut(batchitems); //finish unprocessed batchwrite
		BatchWriteItemOutcome outcome = db.batchWriteItem(tbl);
		return;
	}

	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			iindex = db.createTable(tableName, Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH), // Partition
																												// key
					new KeySchemaElement("inxid", KeyType.RANGE)), // Sort key
					Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S),
							new AttributeDefinition("inxid", ScalarAttributeType.N)),
					new ProvisionedThroughput(100L, 100L));

			iindex.waitForActive();
		} catch (final ResourceInUseException exists) {
			iindex = db.getTable(tableName);
		}

	}

	/**
	 * Given the CSV row and the column names, return the column with a specified
	 * name
	 * 
	 * @param csvRow
	 * @param columnNames
	 * @param columnName
	 * @return
	 */
	public static String lookup(final String[] csvRow, final String[] columnNames, final String columnName) {
		final int inx = Arrays.asList(columnNames).indexOf(columnName);
		
		if (inx < 0)
			throw new RuntimeException("Out of bounds");
		
		return csvRow[inx];
	}
}
