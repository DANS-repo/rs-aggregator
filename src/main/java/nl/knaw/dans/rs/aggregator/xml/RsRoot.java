package nl.knaw.dans.rs.aggregator.xml;

import javax.annotation.Nonnull;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@XmlAccessorType(XmlAccessType.FIELD)
public abstract class RsRoot<T extends RsRoot, C extends RsItem> {

    @XmlElement(name = "md", namespace = "http://www.openarchives.org/rs/terms/")
    private RsMd rsMd;
    @XmlElement(name = "ln", namespace = "http://www.openarchives.org/rs/terms/")
    private List<RsLn> linkList = new ArrayList<>();

    public RsMd getMetadata() {
        return rsMd;
    }

    @SuppressWarnings("unchecked")
    public T withMetadata(@Nonnull RsMd rsMd) {
        this.rsMd = Objects.requireNonNull(rsMd);
        return (T) this;
    }

    public List<RsLn> getLinkList() {
        return linkList;
    }

    public abstract List<C> getItemList();

    @SuppressWarnings("unchecked")
    public T addItem(C item) {
        getItemList().add(item);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T addLink(RsLn rsLn) {
        linkList.add(rsLn);
        return (T) this;
    }

    public String getHref(String rel) {
        for (RsLn rsLn : linkList) {
            if (rel.equals(rsLn.getRel())) {
                return rsLn.getHref();
            }
        }
        return null;
    }

    public Optional<RsLn> getLink(String rel) {
        for (RsLn rsLn : linkList) {
            if (rel.equals(rsLn.getRel())) {
                return Optional.of(rsLn);
            }
        }
        return Optional.empty();
    }

    public int getLevel() {
        return Capability.levelfor(rsMd.getCapability().orElse(""));
    }

    public Optional<Capability> getCapability() {
        try {
            return Optional.of(Capability.forString(rsMd.getCapability().orElse("")));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

}
