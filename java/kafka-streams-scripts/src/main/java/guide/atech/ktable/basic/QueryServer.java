package guide.atech.ktable.basic;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Spark;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.ArrayList;
import java.util.List;

class QueryServer {
    private static final Logger logger = LogManager.getLogger();
    private static final String NO_RESULTS = "No Results Found";
    private static final String APPLICATION_NOT_ACTIVE = "Application is not active. Try later.";
    private final KafkaStreams streams;
    private Boolean isActive = false;
    private final HostInfo hostInfo;
    private final Client client;

    QueryServer(KafkaStreams streams, String hostname, int port) {
        this.streams = streams;
        this.hostInfo = new HostInfo(hostname, port);
        client = ClientBuilder.newClient();
    }

    void setActive(Boolean state) {
        isActive = state;
    }

    private List<KeyValue<String, String>> readAllFromLocal() {

        List<KeyValue<String, String>> localResults = new ArrayList<>();
        ReadOnlyKeyValueStore<String, String> stateStore =
                streams.store(
                        StreamingTableApp.STORE_NAME,
                        QueryableStoreTypes.keyValueStore()
                );

        stateStore.all().forEachRemaining(localResults::add);
        return localResults;
    }

    void start() {
        logger.info("Starting Query Server at http://" + hostInfo.host() + ":" + hostInfo.port()
                + "/" + StreamingTableApp.STORE_NAME + "/all");

        Spark.port(hostInfo.port());

        Spark.get("/" + StreamingTableApp.STORE_NAME + "/all", (req, res) -> {

            List<KeyValue<String, String>> allResults;
            String results;

            if (!isActive) {
                results = APPLICATION_NOT_ACTIVE;
            } else {
                allResults = readAllFromLocal();
                results = (allResults.size() == 0) ? NO_RESULTS
                        : allResults.toString();
            }
            return results;
        });

    }

    void stop() {
        client.close();
        Spark.stop();
    }

}

