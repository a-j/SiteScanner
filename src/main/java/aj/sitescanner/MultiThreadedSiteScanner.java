package aj.sitescanner;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Access a list of web sites and identifies which ones are active/dead/redirect to other sites
 */
public class MultiThreadedSiteScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedSiteScanner.class);

    public static void main( String[] args ) {
        LOGGER.info("Begin processing....");
        final long startTime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(100);
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/UHC_Domains.csv"));

            String str;
            while ((str = reader.readLine()) != null) {
                LOGGER.info("Reading: {}", str);

                SitePinger sitePinger = new SitePinger(str);
                executor.submit(sitePinger);
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.DAYS);
            LOGGER.info("Finished all threads");

            final long endTime = System.currentTimeMillis();
            LOGGER.info("Elapsed Time (in seconds): {}", (endTime - startTime)/1000);
        } catch (InterruptedException ie) {
            LOGGER.error("InterruptedException occurred", ie);
        } catch (IOException ioe) {
            LOGGER.error("IOException occurred", ioe);
        }
    }
}

class SitePinger implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SitePinger.class);
    private final String url;

    SitePinger(String url) {
        this.url = url;
    }

    @Override
    public void run() {
        LOGGER.info("Thread: {} Processing: {} in thread {}", Thread.currentThread().getName(), url);
        try {
            CloseableHttpClient httpClient = HttpClientBuilder.create().disableRedirectHandling().build();
            URI inputUri = new URI(url);
            HttpGet httpGet = new HttpGet(inputUri);

            while (true) {
                CloseableHttpResponse response = null;
                try {
                    response = httpClient.execute(httpGet);
                    int statusCode = response.getStatusLine().getStatusCode();
                    LOGGER.info("Status code: {}", response.getStatusLine().getStatusCode());

                    if (statusCode >= 200 && statusCode < 300) {    // Success
                        saveActiveSite(inputUri.toASCIIString());
                        break;
                    } else if (statusCode >= 301 && statusCode < 400) {
                        URI targetURI = new URI(response.getFirstHeader("Location").getValue());
                        LOGGER.info("Location: {}", targetURI.toASCIIString());
                        // If redirect location is a different path on the same host, follow it
                        if (targetURI.toASCIIString().startsWith("/")) {
                            inputUri = new URI(inputUri.getScheme() + "://" + inputUri.getHost() + targetURI.toASCIIString());
                            httpGet = new HttpGet(inputUri);
                            continue;
                        } else if (!targetURI.toASCIIString().startsWith("http")) {
                            inputUri = new URI(inputUri.getScheme() + "://" + inputUri.getHost() + "/" + targetURI.toASCIIString());
                            httpGet = new HttpGet(inputUri);
                            continue;
                        }
                        if (inputUri.getHost().equalsIgnoreCase(targetURI.getHost())) {
                            // Only protocol has changed - ignore
                            saveActiveSite(inputUri.toASCIIString());
                        } else {
                            saveRedirectSite(inputUri.toASCIIString());
                        }
                        break;
                    } else {
                        saveUnreachableSite(inputUri.toASCIIString());
                        break;
                    }
                } catch (UnknownHostException uhe) {
                    LOGGER.error("UnknownHostException Occurred");
                    saveUnreachableSite(inputUri.toASCIIString());
                    break;
                } catch (HttpHostConnectException ce) {
                    LOGGER.error("HttpHostConnectException Occurred");
                    saveUnreachableSite(inputUri.toASCIIString());
                    break;
                } catch (SocketException se) {
                    LOGGER.error("SocketException Occurred");
                    saveUnreachableSite(inputUri.toASCIIString());
                    break;
                } finally {
                    if (response != null) {
                        response.close();
                    }
                }
            }
        } catch (URISyntaxException use) {
            LOGGER.error("URISyntaxException occurred", use);
        } catch (IOException ioe) {
            LOGGER.error("IOException occurred", ioe);
        }
    }

    private synchronized void saveActiveSite(String url) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/active.csv", true))) {
            writer.write(url);
            writer.newLine();
            writer.flush();
        } catch (IOException ioe) {
            LOGGER.error("IOException Occurred", ioe);
        }
    }

    private synchronized void saveRedirectSite(String url) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/redirect.csv", true))) {
            writer.write(url);
            writer.newLine();
            writer.flush();
        } catch (IOException ioe) {
            LOGGER.error("IOException Occurred", ioe);
        }
    }

    private synchronized void saveUnreachableSite(String url) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/unreachable.csv", true))) {
            writer.write(url);
            writer.newLine();
            writer.flush();
        } catch (IOException ioe) {
            LOGGER.error("IOException Occurred", ioe);
        }
    }
}
