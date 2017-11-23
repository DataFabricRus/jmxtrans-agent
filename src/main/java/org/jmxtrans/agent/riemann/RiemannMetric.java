package org.jmxtrans.agent.riemann;

import io.riemann.riemann.client.RiemannClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class RiemannMetric {
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

    private final String service;
    private final Object value;
    private final long timestampMillis;
    private final ArrayList<String> tags;

    public RiemannMetric(String service, Object value, long timestampMillis, ArrayList<String> tags) {
        this.service = sanitize(Objects.requireNonNull(service));
        this.tags = Objects.requireNonNull(tags);
        this.value = Objects.requireNonNull(value);
        this.timestampMillis = timestampMillis;
    }

    public Object getValue() {
        return valueAsStr();
    }

    private String valueAsStr() {
        if (value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double) {
            return value.toString();
        } else {
            return null;
        }
    }

    public void send(RiemannClient client) throws IOException {
        if (getValue() != null) {
            client
                    .event()
                    .service(service)
                    .metric(Double.parseDouble(getValue().toString()))
                    .time(timestampMillis)
                    .tags(tags)
                    .send()
                    .deref(5000, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, tags, service, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RiemannMetric other = (RiemannMetric) obj;
        return Objects.equals(timestampMillis, other.timestampMillis)
                && Objects.equals(tags, other.tags)
                && Objects.equals(service, other.service)
                && Objects.equals(value, value);
    }

    @Override
    public String toString() {
        return "RiemannMetric [service=" + service + ", value=" + value + ", timestamp=" + timestampMillis +
                ", tags=" + tags + "]";
    }

    private String sanitize(String s) {
        return WHITESPACE.matcher(s).replaceAll("-");
    }
}
