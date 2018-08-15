package nl.knaw.dans.rs.aggregator.sync;

import nl.knaw.dans.rs.aggregator.syncore.VerificationPolicy;
import nl.knaw.dans.rs.aggregator.syncore.VerificationStatus;

/**
 * No verification policy. Use when sources do not include hash, length nor lastmod.
 */
public class NoVerificationPolicy implements VerificationPolicy {

    @Override
    public boolean continueVerification(VerificationStatus stHash, VerificationStatus stLastMod,
                                        VerificationStatus stSize) {
        return false;
    }

    @Override
    public boolean repeatDownload(VerificationStatus stHash, VerificationStatus stLastMod, VerificationStatus stSize) {
        return false;
    }

    @Override
    public boolean isVerified(VerificationStatus stHash, VerificationStatus stLastMod, VerificationStatus stSize) {
        return true;
    }
}
