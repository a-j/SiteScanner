package aj.sitescanner;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Access a list of web sites and identifies which ones are active/dead/redirect to other sites
 */
public class SiteScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SiteScanner.class);
    private static List<String> active = new ArrayList<>();
    private static List<String> redirect = new ArrayList<>();
    private static List<String> unreachable = new ArrayList<>();

    public static void main( String[] args ) {
        LOGGER.info("Begin processing....");
        final long startTime = System.currentTimeMillis();
        CloseableHttpClient httpClient = HttpClientBuilder.create().disableRedirectHandling().build();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/input.txt"));

            String str;
            while ((str = reader.readLine()) != null) {
                LOGGER.info("Processing: {}", str);
                URI inputUri = new URI(str);
                HttpGet httpGet = new HttpGet(inputUri);

                while (true) {
                    CloseableHttpResponse response = null;
                    try {
                        response = httpClient.execute(httpGet);
                        int statusCode = response.getStatusLine().getStatusCode();
                        LOGGER.info("Status code: {}", response.getStatusLine().getStatusCode());

                        if (statusCode >= 200 && statusCode < 300) {    // Success
                            active.add(inputUri.toASCIIString());
                            break;
                        } else if (statusCode >= 301 && statusCode < 400) {
                            URI targetURI = new URI(response.getFirstHeader("Location").getValue());
                            LOGGER.info("Location: {}", targetURI.toASCIIString());
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
                                active.add(inputUri.toASCIIString());
                            } else {
                                redirect.add(inputUri.toASCIIString());
                            }
                            break;
                        } else {
                            unreachable.add(inputUri.toASCIIString());
                            break;
                        }
                    } catch (UnknownHostException uhe) {
                        LOGGER.error("UnknownHostException Occurred");
                        unreachable.add(inputUri.toASCIIString());
                        break;
                    } catch (HttpHostConnectException ce) {
                        LOGGER.error("HttpHostConnectException Occurred");
                        unreachable.add(inputUri.toASCIIString());
                        break;
                    } catch (SocketException se) {
                        LOGGER.error("SocketException Occurred");
                        unreachable.add(inputUri.toASCIIString());
                        break;
                    } finally {
                        if (response != null) {
                            response.close();
                        }
                    }
                }
            }
            LOGGER.info("Active Count: {}", active.size());
            LOGGER.info("Redirect Count: {}", redirect.size());
            LOGGER.info("Unreachable Count: {}", unreachable.size());

            BufferedWriter writer = new BufferedWriter(new FileWriter("data/active.csv"));
            for (String url : active) {
                writer.write(url);
                writer.write(System.lineSeparator());
            }
            writer.flush();

            writer = new BufferedWriter(new FileWriter("data/redirect.csv"));
            for (String url : redirect) {
                writer.write(url);
                writer.write(System.lineSeparator());
            }
            writer.flush();

            writer = new BufferedWriter(new FileWriter("data/unreachable.csv"));
            for (String url : unreachable) {
                writer.write(url);
                writer.write(System.lineSeparator());
            }
            writer.flush();
            writer.close();
            final long endTime = System.currentTimeMillis();
            LOGGER.info("Elapsed Time (in seconds): {}", (endTime - startTime)/1000);

        } catch (IOException ioe) {
            LOGGER.error("IOException occurred", ioe);
        } catch (URISyntaxException use) {
            LOGGER.error("URISyntaxException occurred", use);
        }
    }
}
