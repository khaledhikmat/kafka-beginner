package kafka.elastic;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticConsumer {
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticConsumer.class.getName());
        RestHighLevelClient client = createClient();
        String json = "{\"foo\": \"bar\"}";
        IndexRequest indexReq = new IndexRequest("twitter", "tweets").source(json, XContentType.JSON);
        IndexResponse indexResponse = client.index(indexReq, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info("Elastic index doc id: " + id);
        client.close();
    }

    public static RestHighLevelClient createClient() {
        String hostname = "kafka-course-4200270804.us-east-1.bonsaisearch.net";
        String username = "jaxyba9adj";
        String password = "sby3g5yx6i";

        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credsProvider);
                }
            });

        return new RestHighLevelClient(builder);
    }
}
