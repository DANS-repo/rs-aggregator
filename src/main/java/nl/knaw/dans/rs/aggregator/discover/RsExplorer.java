package nl.knaw.dans.rs.aggregator.discover;

import nl.knaw.dans.rs.aggregator.http.RemoteResourceSyncFrameworkException;
import nl.knaw.dans.rs.aggregator.http.Result;
import nl.knaw.dans.rs.aggregator.util.LambdaUtil;
import nl.knaw.dans.rs.aggregator.xml.Capability;
import nl.knaw.dans.rs.aggregator.xml.ResourceSyncContext;
import nl.knaw.dans.rs.aggregator.xml.RsBuilder;
import nl.knaw.dans.rs.aggregator.xml.RsItem;
import nl.knaw.dans.rs.aggregator.xml.RsMd;
import nl.knaw.dans.rs.aggregator.xml.RsRoot;
import nl.knaw.dans.rs.aggregator.xml.Sitemapindex;
import nl.knaw.dans.rs.aggregator.xml.Urlset;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Download ResourceSync Framework documents.
 */
public class RsExplorer extends AbstractUriExplorer {

  private final ResourceSyncContext rsContext;

  private LambdaUtil.BiFunction_WithExceptions<URI, HttpResponse, RsRoot, Exception> converter;

  public boolean followParentLinks = true;
  public boolean followIndexLinks = true;
  public boolean followChildLinks = true;

  public RsExplorer(CloseableHttpClient httpClient, ResourceSyncContext rsContext) {
    super(httpClient);
    this.rsContext = rsContext;
  }

  public RsExplorer withFollowParentLinks(boolean follow) {
    followParentLinks = follow;
    return this;
  }

  public RsExplorer withFollowIndexLinks(boolean follow) {
    followIndexLinks = follow;
    return this;
  }

  public RsExplorer withFollowChildLinks(boolean follow) {
    followChildLinks = follow;
    return this;
  }

  public RsExplorer withConverter(LambdaUtil.BiFunction_WithExceptions<URI, HttpResponse, RsRoot, Exception> converter) {
    this.converter = converter;
    return this;
  }

  public ResultIndex explore(URI uri) {
    ResultIndex index = new ResultIndex();
    explore(uri, index);
    return index;
  }

  @SuppressWarnings ("unchecked")
  @Override
  public Result<RsRoot> explore(URI uri, ResultIndex index) {
    Result<RsRoot> result = execute(uri, getConverter());
    index.add(result);
    Capability capability = extractCapability(result);

    if (followParentLinks) {
      // rs:ln rel="up" -> points to parent document, a urlset.
      String parentLink = result.getContent().map(rsRoot -> rsRoot.getLink("up")).orElse(null);
      if (parentLink != null && !index.contains(parentLink)) {
        try {
          URI parentUri = new URI(parentLink);
          Result<RsRoot> parentResult = explore(parentUri, index);
          result.addParent(parentResult);
          verifyUpRelation(result, parentResult, capability);
        } catch (URISyntaxException e) {
          index.addInvalidUri(parentLink);
          result.addError(e);
          result.addInvalidUri(parentLink);
        }
      }
    }

    if (followIndexLinks) {
      // rs:ln rel="index" -> points to parent index, a sitemapindex.
      String indexLink = result.getContent().map(rsRoot -> rsRoot.getLink("index")).orElse(null);
      if (indexLink != null && !index.contains(indexLink)) {
        try {
          URI indexUri = new URI(indexLink);
          Result<RsRoot> indexResult = explore(indexUri, index);
          result.addParent(indexResult);
          verifyIndexRelation(result, indexResult, capability);
        } catch (URISyntaxException e) {
          index.addInvalidUri(indexLink);
          result.addError(e);
          result.addInvalidUri(indexLink);
        }
      }
    }

    if (followChildLinks) {
      // elements <url> or <sitemap> have the location of the children of result.
      // children of Urlset with capability resourcelist, resourcedump, changelist, changedump
      // are the resources them selves. do not explore these with this explorer.
      String xmlString = result.getContent()
        .map(RsRoot::getMetadata).flatMap(RsMd::getCapability).orElse("invalid");

      boolean isSitemapindex = result.getContent().map(rsRoot -> rsRoot instanceof Sitemapindex).orElse(false);

      if (Capability.levelfor(xmlString) > Capability.RESOURCELIST.level || isSitemapindex) {
        List<RsItem> itemList = result.getContent().map(RsRoot::getItemList).orElse(Collections.emptyList());
        for (RsItem item : itemList) {
          String childLink = item.getLoc();
          if (childLink != null && !index.contains(childLink)) {
            try {
              URI childUri = new URI(childLink);
              Result<RsRoot> childResult = explore(childUri, index);
              result.addChild(childResult);
              verifyChildRelation(result, childResult, capability);
            } catch (URISyntaxException e) {
              index.addInvalidUri(childLink);
              result.addError(e);
              result.addInvalidUri(childLink);
            }
          }
        }
      }
    }

    return result;
  }

  private Capability extractCapability(Result<RsRoot> result) {
    String xmlString = result.getContent()
      .map(RsRoot::getMetadata).flatMap(RsMd::getCapability).orElse("");
    Capability capa = null;
    try {
      capa = Capability.forString(xmlString);
    } catch (IllegalArgumentException e) {
      result.addError(new RemoteResourceSyncFrameworkException(
        String.format("invalid value for capability: '%s'", xmlString)));
    }
    return capa;
  }

  private void verifyUpRelation(Result<RsRoot> result, Result<RsRoot> parentResult, Capability capability) {
    if (result.getContent().isPresent() && parentResult.getContent().isPresent()) {
      Capability parentCapa = extractCapability(parentResult);

      if (capability != null && !capability.verifyUpRelation(parentCapa)) {
        result.addError(new RemoteResourceSyncFrameworkException(
          String.format("invalid up relation: Expected '%s', found '%s'",
            capability.getUpRelation() == null ? "<no relation>" : capability.getUpRelation().xmlValue,
            parentCapa == null ? "<no relation>" : parentCapa.xmlValue)));
      }
    }

    // up relation is always to a urlset
    if (!parentResult.getContent().map(content -> content instanceof Urlset).orElse(true)) {
      result.addError(new RemoteResourceSyncFrameworkException(
        "invalid up relation: parent document is not '<urlset>'"));
    }

  }

  private void verifyIndexRelation(Result<RsRoot> result, Result<RsRoot> parentResult, Capability capability) {
    if (result.getContent().isPresent() && parentResult.getContent().isPresent()) {
      Capability parentCapa = extractCapability(parentResult);

      if (capability != null && !capability.verifyIndexRelation(parentCapa)) {
        result.addError(new RemoteResourceSyncFrameworkException(
          String.format("invalid index relation: Expected '%s', found '%s'",
            capability.getIndexRelation() == null ? "<no relation>" : capability.getIndexRelation().xmlValue,
            parentCapa == null ? "<no relation>" : parentCapa.xmlValue)));
      }
    }

    // index relation is always to a sitemapindex
    if (!parentResult.getContent().map(content -> content instanceof Sitemapindex).orElse(true)) {
      result.addError(new RemoteResourceSyncFrameworkException(
        "invalid index relation: parent document is not '<sitemapindex>'"));
    }
  }

  private void verifyChildRelation(Result<RsRoot> result, Result<RsRoot> childResult, Capability capability) {
    if (result.getContent().isPresent() && childResult.getContent().isPresent()) {
      Capability childCapa = extractCapability(childResult);

      if (capability != null && !capability.verifyChildRelation(childCapa)) {
        result.addError(new RemoteResourceSyncFrameworkException(
          String.format("invalid child relation: Expected %s, found '%s'",
            Arrays.toString(capability.getChildRelationsXmlValues()),
            childCapa == null ? "<no relation>" : childCapa.xmlValue)));
      }

      // child relation to document of same capability only allowed if document is sitemapIndex
      if (capability != null && capability == childCapa) {
        if (!result.getContent().map(content -> content instanceof Sitemapindex).orElse(true)) {
          result.addError(new RemoteResourceSyncFrameworkException(
            String.format("invalid child relation: relation to same capability '%s' " +
              "and document is not '<sitemapindex>'", capability.xmlValue)));
        }
      }
    }
  }

  private ResourceSyncContext getRsContext() {
    return rsContext;
  }

  private LambdaUtil.BiFunction_WithExceptions<URI, HttpResponse, RsRoot, Exception> rsConverter = (uri, response) -> {
    InputStream inStream = response.getEntity().getContent();
    return new RsBuilder(this.getRsContext()).setInputStream(inStream).build().orElse(null);
  };

  private LambdaUtil.BiFunction_WithExceptions<URI, HttpResponse, RsRoot, Exception> getConverter() {
    if (converter == null) {
      converter = rsConverter;
    }
    return converter;
  }

}
