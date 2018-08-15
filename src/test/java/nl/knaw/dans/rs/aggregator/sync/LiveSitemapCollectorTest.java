package nl.knaw.dans.rs.aggregator.sync;

import nl.knaw.dans.rs.aggregator.http.Testing;
import nl.knaw.dans.rs.aggregator.syncore.PathFinder;
import nl.knaw.dans.rs.aggregator.syncore.SitemapConverterProvider;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import nl.knaw.dans.rs.aggregator.xml.UrlItem;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

/**
 * Created on 2017-04-26 10:43.
 */
public class LiveSitemapCollectorTest {

    private static String baseDirectory = "target/test-output/sitemapcollector";
    //private static String capabilityListUrl = "http://zandbak11.dans.knaw.nl/ehri2/mdx/capabilitylist.xml";
    private static String capabilityListUrl =
      "https://data.anansi.clariah.nl/v5/resourcesync/u74ccc032adf8422d7ea92df96cd4783f0543db3b" +
        "/gemeentegeschiedenisnl/capabilitylist.xml";
    //private static String capabilityListUrl = "https://data.anansi.clariah.nl/v5/resourcesync/sourceDescription.xml";
    // private static String capabilityListUrl =
    //   "http://publisher-connector.core.ac.uk/resourcesync/sitemaps/elsevier/pdf/capabilitylist.xml";

    @BeforeClass
    public static void initialize() throws Exception {
        assumeTrue(Testing.LIVE_TESTS);
    }

    @Test
    public void testCollectSitemaps() throws Exception {
        PathFinder pathFinder = new PathFinder(baseDirectory, URI.create(capabilityListUrl));
        RsProperties syncProps = new RsProperties();
        SitemapConverterProvider provider = new FsSitemapConverterProvider();
        provider.setPathFinder(pathFinder);
        SitemapCollector collector = new SitemapCollector()
          .withConverter(provider.getConverter());
        collector.collectSitemaps(pathFinder, syncProps);
        assertThat(collector.getCountCapabilityLists(), is(1));

        System.out.println("\nMost recent items:");
        for (Map.Entry<URI, UrlItem> entry : collector.getMostRecentItems().entrySet()) {
            System.out.println(entry.getKey() + " at " + entry.getValue().getRsMdAt());
        }

    }
}
