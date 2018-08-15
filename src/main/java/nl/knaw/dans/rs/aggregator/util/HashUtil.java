package nl.knaw.dans.rs.aggregator.util;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Compute the hash over inputStreams.
 */
public class HashUtil {

    public static String computeHash(String algorithm, @Nonnull InputStream inputStream)
      throws NoSuchAlgorithmException, IOException {

        StringBuilder sb;
        try (InputStream ins = inputStream) {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] byteArray = new byte[1024];
            int bytesCount = 0;
            while ((bytesCount = ins.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }
            byte[] bytes = digest.digest();
            sb = new StringBuilder();
            for (byte aByte : bytes) {
                sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
            }
        }
        return sb.toString();
    }

    public static String computeHash(String algorithm, @Nonnull File file)
      throws IOException, NoSuchAlgorithmException {
        BufferedInputStream buff = new BufferedInputStream(new FileInputStream(file));
        return computeHash(algorithm, buff);
    }
}
