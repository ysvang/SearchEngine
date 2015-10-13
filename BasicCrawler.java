/**
 * Web Crawler backend
 * Author: Scott Vang
 */

package edu.uci.ics.crawler4j.examples.basic;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

import java.rmi.UnknownHostException;
import java.util.List;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.mongodb.*;





public class BasicCrawler extends WebCrawler{

  private final static Pattern BINARY_FILES_EXTENSIONS =
        Pattern.compile(".*\\.(bmp|gif|jpe?g|png|tiff?|pdf|ico|xaml|pict|rif|pptx?|ps" +
        "|mid|mp2|mp3|mp4|wav|wma|au|aiff|flac|ogg|3gp|aac|amr|au|vox" +
        "|avi|mov|mpe?g|ra?m|m4v|smil|wm?v|swf|aaf|asf|flv|mkv" +
        "|zip|rar|gz|7z|aac|ace|alz|apk|arc|arj|dmg|jar|lzip|lha" +
        "|js|css)" +
        "(\\?.*)?$"); // For url Query parts ( URL?q=... )
  private final static Pattern VALID_SUBDOMAINS = Pattern.compile("^http://.*\\.ics\\.uci\\.edu/.*");
  private final static Pattern INVALID_SUBDOMAINS = Pattern.compile("^http://(calendar|ftp|fano)\\.ics\\.uci\\.edu.*");
  private final static Pattern INVALID_ARCHIVE_SITES = Pattern.compile("^http://archive.ics.uci.edu/ml/datasets.*");
  private final static Pattern INVALID_QUERY_SITES = Pattern.compile(".*[\\?].*");

  
  //DB db = mongoClient.getDB("ics_db");
  
  /*
   * Crawler logic
   */
  @Override
  public boolean shouldVisit(WebURL url) {
    String href = url.getURL().toLowerCase();
    System.out.println(href);
    return !BINARY_FILES_EXTENSIONS.matcher(href).matches() && VALID_SUBDOMAINS.matcher(href).matches()
    														&& !INVALID_SUBDOMAINS.matcher(href).matches()
    														&& !INVALID_ARCHIVE_SITES.matcher(href).matches()
    														&& !INVALID_QUERY_SITES.matcher(href).matches();
  }
 
  /*
   * This function is called when a page is fetched and ready to be processed
   */
  @Override
  public void visit(Page page) {
    int docid = page.getWebURL().getDocid();
    String url = page.getWebURL().getURL();
    String domain = page.getWebURL().getDomain();
    String path = page.getWebURL().getPath();
    String subDomain = page.getWebURL().getSubDomain();
    String parentUrl = page.getWebURL().getParentUrl();
    String anchor = page.getWebURL().getAnchor();

    System.out.println("Docid: " + docid);
    System.out.println("URL: " + url);
    System.out.println("Domain: " + domain);
    System.out.println("Sub-domain: " + subDomain);
    System.out.println("Path: " + path);
    System.out.println("Parent page: " + parentUrl);
    System.out.println("Anchor text: " + anchor);

    if (page.getParseData() instanceof HtmlParseData) {
      HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
      String text = htmlParseData.getText();
      String html = htmlParseData.getHtml();
      int text_length = text.length();
      List<WebURL> links = htmlParseData.getOutgoingUrls();

      System.out.println("Text length: " + text_length);
      System.out.println("Title: " + htmlParseData.getTitle());
      System.out.println("Html length: " + html.length());
      //System.out.println("Number of outgoing links: " + links.size());
      writeOutFiles(docid, url, domain, subDomain, path, anchor, parentUrl, html, text_length);
    }
  }
 
 
  
  // Write out discovered URLs to mongoDB.
  public void writeOutFiles(int docid, String url, String domain, String subDomain, String path, String anchor, String parentUrl, String text, int text_length){
	  MongoClient mongoClient = null;
	  DB db = null;
	  try{
		  mongoClient = new MongoClient();
	      db = mongoClient.getDB("ics_db2");
	  } catch (Exception e) {
	    	e.printStackTrace();
	  }
	  
	  DBCollection table = db.getCollection("webpages2");
	  BasicDBObject doc = new BasicDBObject("docid", docid)
	  	.append("url", url)
	  	.append("domain", domain)
	  	.append("subDomain", subDomain)
	  	.append("path", path)
	  	.append("anchor", anchor)
	  	.append("parentURL", parentUrl)
	  	.append("text", text);
	  try{
		  table.insert(doc);
	  } catch (Exception e) {
		  mongoClient.close();
	  }
	  mongoClient.close();	
  }
   
	  

  
}