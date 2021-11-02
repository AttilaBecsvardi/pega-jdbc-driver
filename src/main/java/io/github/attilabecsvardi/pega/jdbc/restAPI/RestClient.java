package io.github.attilabecsvardi.pega.jdbc.restAPI;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.cxf.configuration.jsse.TLSClientParameters;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class RestClient {

    private static final String PEGA_JDBC_API_URI_PATH = "/PegaJDBC";
    private static final String TERMINATE_REQUESTOR_URI_PATH = "/TerminateRequestor";

    private WebClient client;
    private String url;
    private String baseUrl;

    public RestClient(String url, Properties info) {

        this.url = url;
        this.baseUrl = url.substring(0, url.lastIndexOf(PEGA_JDBC_API_URI_PATH));

        final List<Object> providers = new ArrayList<>();
        JacksonJaxbJsonProvider jacksonJsonProvider = new JacksonJaxbJsonProvider();
        providers.add(jacksonJsonProvider);
        this.client = WebClient.create(baseUrl, providers, info.getProperty("user"), info.getProperty("password"), null);
        WebClient.getConfig(client).getRequestContext().put(
                org.apache.cxf.message.Message.MAINTAIN_SESSION, Boolean.TRUE);

        // trust all certs
        {
            HTTPConduit conduit = WebClient.getConfig(client).getHttpConduit();

            TLSClientParameters params = conduit.getTlsClientParameters();

            if (params == null) {
                params = new TLSClientParameters();
                conduit.setTlsClientParameters(params);
            }

            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    //LOGGER.info("accept all issuer");
                    return null;
                }

                @Override
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                    //LOGGER.info("checkClientTrusted");
                    // Trust everything
                }

                @Override
                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                    //LOGGER.info("checkServerTrusted");
                    // Trust everything
                }
            }};

            params.setTrustManagers(trustAllCerts);
            params.setDisableCNCheck(true);
        }
    }


    public Response invokeJDBCMethod(String instanceType, String instanceID, JDBCMethod method) {
        /*
        // debug
        System.err.println(System.currentTimeMillis() +
                " - instanceType: " + instanceType +
                ", instanceID: " + instanceID +
                ", methodName: " + method.getMethodName() +
                ", retInstanceID: " + method.getRetInstanceID());
        // debug end
        */


        // If path starts from "/" then all the current path starting from the base URI
        // will be replaced, otherwise only the last path segment will be replaced.
        return client
                .replacePath(PEGA_JDBC_API_URI_PATH)
                .path(instanceType)
                .path(instanceID)
                .type(MediaType.APPLICATION_JSON)
                .post(Entity.entity(method, MediaType.APPLICATION_JSON));

    }

    public Response terminateRequestor() {

        return client
                .replacePath(TERMINATE_REQUESTOR_URI_PATH)
                .type(MediaType.APPLICATION_JSON)
                .delete();
    }
}
