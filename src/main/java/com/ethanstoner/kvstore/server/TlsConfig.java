package com.ethanstoner.kvstore.server;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * Immutable TLS configuration holder built from a JKS keystore.
 *
 * <p>Usage:
 * <pre>
 *   TlsConfig tls = TlsConfig.fromKeystore(Path.of("server.jks"), "password".toCharArray());
 *   KvServer server = new KvServer(store, 6380, null, tls);
 * </pre>
 *
 * <p>PEM-based configuration (--tls-cert / --tls-key) is future work.
 * The standard JDK approach is to convert PEM files to a JKS keystore:
 * <pre>
 *   openssl pkcs12 -export -in cert.pem -inkey key.pem -out server.p12 -name kvstore
 *   keytool -importkeystore -srckeystore server.p12 -srcstoretype PKCS12 \
 *           -destkeystore server.jks -deststoretype JKS
 * </pre>
 */
public final class TlsConfig {

    private final SSLServerSocketFactory factory;

    private TlsConfig(SSLServerSocketFactory factory) {
        this.factory = factory;
    }

    /**
     * Loads a JKS keystore from disk and builds an SSLServerSocketFactory
     * configured for TLSv1.3.
     *
     * @param keystorePath path to the JKS keystore file
     * @param password     keystore and key password
     * @return a ready-to-use {@code TlsConfig}
     * @throws IOException              if the keystore file cannot be read
     * @throws GeneralSecurityException if the keystore cannot be initialised
     *                                  (wrong password, corrupt data, etc.)
     */
    public static TlsConfig fromKeystore(Path keystorePath, char[] password)
            throws IOException, GeneralSecurityException {
        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream(keystorePath.toFile())) {
            ks.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password);
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(kmf.getKeyManagers(), null, null);
        return new TlsConfig(ctx.getServerSocketFactory());
    }

    /** Returns the {@link SSLServerSocketFactory} to use when creating the server socket. */
    public SSLServerSocketFactory factory() {
        return factory;
    }
}
