import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.spark.connector.cql.CassandraConnectionFactory;
import com.datastax.spark.connector.cql.CassandraConnectorConf;
import com.datastax.spark.connector.cql.LocalNodeFirstLoadBalancingPolicy;
import com.datastax.spark.connector.cql.MultipleRetryPolicy;
import scala.collection.immutable.HashSet;
import scala.collection.immutable.Set;
import scala.reflect.ClassTag;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

public class CustomCassandraConnectionFactory implements CassandraConnectionFactory {
    @Override
    public Cluster createCluster(CassandraConnectorConf conf)  {
        try {
            return clusterBuilder (conf).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> properties() {
        try {
            return new HashSet<String>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Cluster.Builder clusterBuilder(CassandraConnectorConf conf) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(conf.connectTimeoutMillis());
        socketOptions.setReadTimeoutMillis(conf.readTimeoutMillis());

        List<Inet4Address> hosts = new ArrayList<Inet4Address>();
        scala.collection.Iterator iter = conf.hosts().toIterator();
        while (iter.hasNext()) {
            Inet4Address a = (Inet4Address) iter.next();
            hosts.add(a);
        }

        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(hosts.toArray(new Inet4Address[0]))
                .withPort(conf.port())
                .withRetryPolicy(
			new MultipleRetryPolicy(conf.queryRetryCount()))
                .withReconnectionPolicy(
                        new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis(), conf.maxReconnectionDelayMillis()))
                .withLoadBalancingPolicy(
                        new LocalNodeFirstLoadBalancingPolicy(conf.hosts(), conf.localDC(), true))
                .withAuthProvider(conf.authConf().authProvider())
                .withSocketOptions(socketOptions)
                .withCompression(conf.compression());

        if (conf.cassandraSSLConf().enabled()) {
            SSLOptions options = createSSLOPtions(conf.cassandraSSLConf());
            if (null != options) {
                builder = builder.withSSL(options);
            } else {
                builder = builder.withSSL();
            }
        }
        return builder;
    }

    SSLOptions createSSLOPtions (CassandraConnectorConf.CassandraSSLConf conf) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
	if (conf.trustStorePath().isEmpty()) {
            return null;
        }
        try (InputStream trustStore = this.getClass().getClassLoader().getResourceAsStream(conf.trustStorePath().get())) {
                KeyStore keyStore = KeyStore.getInstance(conf.trustStoreType());
                keyStore.load(trustStore, conf.trustStorePassword().isDefined() ? conf.trustStorePassword().get().toCharArray() : null);

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(keyStore);

                SSLContext context = SSLContext.getInstance(conf.protocol());
                context.init(null, tmf.getTrustManagers(), new SecureRandom());

                ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

                return JdkSSLOptions.builder()
                        .withSSLContext(context)
                        .withCipherSuites((String[]) conf.enabledAlgorithms().toArray(tag)).build();
            }
        }
}
