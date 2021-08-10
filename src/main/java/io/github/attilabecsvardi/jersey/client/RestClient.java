package io.github.attilabecsvardi.jersey.client;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;

public class RestClient {

    private Client client;
    private String url;


    public RestClient(String url) {
        ClientConfig config = new ClientConfig();
        config.connectorProvider(new ApacheConnectorProvider());
        config.property(ApacheClientProperties.DISABLE_COOKIES, false);

        this.client = ClientBuilder.newClient(config);
        this.url = url;
    }

    @POST
    public Response invokeJDBCMethod(String instanceName, JDBCMethod method) throws Exception {
        return client
                .target(url)
                .path(instanceName)
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(method, MediaType.APPLICATION_JSON));
    }
}
