/**
 * Controller for the web crawler.
 * Author: Scott Vang
 */

package edu.uci.ics.crawler4j.examples.basic;



import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


public class BasicCrawlController {

  public static void main(String[] args) throws Exception {

	    
	 	  
    /*
     * crawlStorageFolder is a folder where intermediate crawl data is
     * stored.
     */
     String crawlStorageFolder = "C:/cs221/";

    /*
     * numberOfCrawlers shows the number of concurrent threads that should
     * be initiated for crawling.
     */
     int numberOfCrawlers = 20;

    CrawlConfig config = new CrawlConfig();

    /*
	 * Set User Agent to my Student ID
	 */
	config.setUserAgentString("UCI WebCrawler");
	 
    config.setCrawlStorageFolder(crawlStorageFolder);
    
    /*
     * Be polite: Make sure that we don't send more than 1 request per
     * second (500 milliseconds between requests).
     */
    config.setPolitenessDelay(300);

    /*
     * You can set the maximum crawl depth here. The default value is -1 for
     * unlimited depth
     */
    config.setMaxDepthOfCrawling(-1);

    /*
     * You can set the maximum number of pages to crawl. The default value
     * is -1 for unlimited number of pages
     */
    config.setMaxPagesToFetch(-1);

    /**
     * Do you want crawler4j to crawl also binary data ?
     * example: the contents of pdf, or the metadata of images etc
     */
    config.setIncludeBinaryContentInCrawling(false);


    /*
     * This config parameter can be used to set your crawl to be resumable
     * (meaning that you can resume the crawl from a previously
     * interrupted/crashed crawl). Note: if you enable resuming feature and
     * want to start a fresh crawl, you need to delete the contents of
     * rootFolder manually.
     */
    config.setResumableCrawling(true);


    /*
     * Instantiate the controller for this crawl.
     */
    PageFetcher pageFetcher = new PageFetcher(config);
    RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
    RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
    CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

    /*
     * For each crawl, you need to add some seed urls. These are the first
     * URLs that are fetched and then the crawler starts following links
     * which are found in these pages
     */
    controller.addSeed("http://www.ics.uci.edu/");

    /*
     * Start the crawl. This is a blocking operation, meaning that your code
     * will reach the line after this only when crawling is finished.
     */
    controller.start(BasicCrawler.class, numberOfCrawlers);
  }
}