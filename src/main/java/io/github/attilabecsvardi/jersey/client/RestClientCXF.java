package io.github.attilabecsvardi.jersey.client;
/*
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.cxf.jaxrs.client.WebClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
*/

public class RestClientCXF {
/*
    private static final String PEGA_JDBC_API_URI_PATH = "/PegaJDBC";
    private static final String TERMINATE_REQUESTOR_URI_PATH = "/TerminateRequestor";

    private WebClient client;
    private String url;
    private String baseUrl;

    public RestClientCXF(String url, Properties info) {
        final List<Object> providers = new ArrayList<>();
        JacksonJaxbJsonProvider jacksonJsonProvider = new JacksonJaxbJsonProvider();
        providers.add( jacksonJsonProvider );
        this.client = WebClient.create(url, providers, info.getProperty("user"), info.getProperty("password"), null);
        WebClient.getConfig(client).getRequestContext().put(
                org.apache.cxf.message.Message.MAINTAIN_SESSION, Boolean.TRUE);
        this.url = url;
        this.baseUrl = url.substring(0, url.lastIndexOf(PEGA_JDBC_API_URI_PATH));


    }


    public Response invokeJDBCMethod(String instanceType, String instanceID, JDBCMethod method) throws Exception {
        // debug
        /*System.err.println(System.currentTimeMillis() +
                " - instanceType: " + instanceType +
                ", instanceID: " + instanceID +
                ", methodName: " + method.getMethodName() +
                ", retInstanceID: " + method.getRetInstanceID());*/
    // debug end
/*
        return client
                .path(instanceType)
                .path(instanceID)
                .type(MediaType.APPLICATION_JSON)
                .post(Entity.entity(method, MediaType.APPLICATION_JSON));
    }

    public Response terminateRequestor() throws Exception {

        return client
                .path(TERMINATE_REQUESTOR_URI_PATH)
                .type(MediaType.APPLICATION_JSON)
                .delete();
    }*/
}
