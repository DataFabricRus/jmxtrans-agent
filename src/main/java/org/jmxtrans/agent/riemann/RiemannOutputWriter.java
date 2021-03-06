package org.jmxtrans.agent.riemann;

import io.riemann.riemann.client.RiemannClient;
import org.jmxtrans.agent.AbstractOutputWriter;
import org.jmxtrans.agent.util.net.HostAndPort;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.jmxtrans.agent.util.ConfigurationUtils.getInt;
import static org.jmxtrans.agent.util.ConfigurationUtils.getString;

/**
 * Output writer for writing to Riemann server.
 *
 * @author <a href="mailto:sergey@osipoff.name">Sergey Osipov</a>
 * @see <a href="http://riemann.io/">Riemann monitors distributed systems.</a>
 */
public class RiemannOutputWriter extends AbstractOutputWriter {

    private static final String RIEMANN_HOST = "RIEMANN_HOST";
    private static final String RIEMANN_PORT = "RIEMANN_PORT";
    private static final String RIEMANN_TAGS = "RIEMANN_TAGS";
    private HostAndPort riemannServerHostAndPort;
    private RiemannClient client;

    private ArrayList<String> tags = new ArrayList<>();

    @Override
    public void postConstruct(@Nonnull Map<String, String> settings) {
        super.postConstruct(settings);

        String riemannHost;
        try {
            riemannHost = getString(settings, "host");
            if (riemannHost == null || riemannHost.isEmpty()) {
                throw new IllegalArgumentException("Setting host is empty");
            }
        } catch (IllegalArgumentException e) {
            logger.log(getInfoLevel(), "Riemann host is not defined in config file. Trying to get from env.");
            riemannHost = getEnvVar(RIEMANN_HOST);
        }

        Integer riemannPort;
        try {
            riemannPort = getInt(settings, "port");
        } catch (IllegalArgumentException e) {
            logger.log(getInfoLevel(), "Riemann port is not defined in config file. Trying to get from env.");
            riemannPort = Integer.parseInt(Objects.requireNonNull(getEnvVar(RIEMANN_PORT)));
        }

        riemannServerHostAndPort = new HostAndPort(riemannHost, riemannPort);

        String tags;
        try {
            tags = getString(settings, "tags");
            if (tags == null || tags.isEmpty()) {
                throw new IllegalArgumentException("Setting tags is empty");
            }
        } catch (IllegalArgumentException e) {
            logger.log(getInfoLevel(), "Riemann tags is not defined in config file. Trying to get from env.");
            tags = getEnvVar(RIEMANN_TAGS);
        }
        for (String tag : tags.split(",")) {
            this.tags.add(tag.trim());
        }

        logger.log(getInfoLevel(), "RiemannOutputWriter is configured with " + riemannServerHostAndPort);
    }

    @Override
    public void writeInvocationResult(@Nonnull String invocationName, @Nullable Object value) throws IOException {
        writeQueryResult(invocationName, null, value);
    }

    @Override
    public void writeQueryResult(@Nonnull String metricName, @Nullable String metricType, @Nullable Object value)
            throws IOException {

        RiemannMetric metric = new RiemannMetric(
                metricName,
                value,
                TimeUnit.SECONDS.convert(
                        System.currentTimeMillis(),
                        TimeUnit.MILLISECONDS),
                tags);

        try {
            ensureRiemannConnection();
            if (logger.isLoggable(getTraceLevel())) {
                logger.log(getTraceLevel(), "Send '" + metric.toString() + "' to " + riemannServerHostAndPort);
            }

            metric.send(client);

        } catch (IOException e) {
            logger.log(Level.WARNING, "Exception sending '" + metric.toString() + "' to " + riemannServerHostAndPort, e);
            releaseRiemannConnection();
            throw e;
        }
    }

    @Override
    public void postCollect() throws IOException {
        super.postCollect();
    }

    @Override
    public void preCollect() throws IOException {
        super.preCollect();
    }

    @Override
    public void preDestroy() {
        super.preDestroy();
        releaseRiemannConnection();
    }

    private void ensureRiemannConnection() throws IOException {
        boolean isConnected;
        try {
            isConnected = client != null &&
                    client.isConnected();
        } catch (Exception e) {
            isConnected = false;
        }
        if (!isConnected) {
            try {
                client = RiemannClient.tcp(riemannServerHostAndPort.getHost(), riemannServerHostAndPort.getPort());
                client.connect();
            } catch (IOException e) {
                ConnectException ce = new ConnectException("Exception connecting to " + riemannServerHostAndPort);
                ce.initCause(e);
                throw ce;
            }
        }
    }

    private void releaseRiemannConnection() {
        if (client.isConnected()) {
            client.close();
        }
    }

    /**
     * @param name
     * @return null if env var doesn't exist
     */
    private String getEnvVar(String name) {
        final String upperCaseName = name.toUpperCase();
        if (System.getenv().containsKey(upperCaseName)) {
            return System.getenv(upperCaseName);
        }

        final String lowerCaseName = name.toLowerCase();
        if (System.getenv().containsKey(lowerCaseName)) {
            return System.getenv(lowerCaseName);
        }

        return null;
    }
}
