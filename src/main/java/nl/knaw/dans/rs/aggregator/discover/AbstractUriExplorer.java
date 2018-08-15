package nl.knaw.dans.rs.aggregator.discover;


import nl.knaw.dans.rs.aggregator.http.AbstractUriReader;
import nl.knaw.dans.rs.aggregator.http.Result;
import org.apache.http.impl.client.CloseableHttpClient;

import java.net.URI;

/**
 * Explore URI's and store {@link Result}s in a {@link ResultIndex}.
 */
public abstract class AbstractUriExplorer extends AbstractUriReader {

    public AbstractUriExplorer(CloseableHttpClient httpClient) {
        super(httpClient);
    }

    public abstract Result<?> explore(URI uri, ResultIndex index);

}
