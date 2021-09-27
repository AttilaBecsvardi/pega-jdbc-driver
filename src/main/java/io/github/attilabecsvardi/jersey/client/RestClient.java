package io.github.attilabecsvardi.jersey.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.util.JacksonFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyInvocation;
import org.glassfish.jersey.client.JerseyWebTarget;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;

public class RestClient {

    private Client client;
    private String url;

    public RestClient(String url, Properties info) {
        ClientConfig config;


        ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

        // DEFAULT_MAPPER.setSerializationInclusion(Inclusion.NON_NULL);
        //DEFAULT_MAPPER.enable(SerializationConfig.Feature.INDENT_OUTPUT);
        //DEFAULT_MAPPER.enable(DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        //DEFAULT_MAPPER.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
        DEFAULT_MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        DEFAULT_MAPPER.enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
        DEFAULT_MAPPER.enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER.mappedFeature());

        //config.register(new JacksonJsonProvider(DEFAULT_MAPPER));
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(DEFAULT_MAPPER);
        config = new ClientConfig(provider);


        config.connectorProvider(new ApacheConnectorProvider());
        config.property(ApacheClientProperties.DISABLE_COOKIES, false);


        // authentication
        if (info != null) {
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(info.getProperty("user"), info.getProperty("password"));
            config.register(feature);
            config.register(JacksonFeature.class);
        }

        SSLContext sslConfig = sslContext();

        ClientBuilder cb = ClientBuilder.newBuilder();
        cb.withConfig(config);
        cb.sslContext(sslConfig);


        //this.client = ClientBuilder.newClient(config);
        this.client = cb.build();


        this.url = url;
    }

    public static SSLContext sslContext() {
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
        try {
            // Install the all-trusting trust manager
            SSLContext sc = SslConfigurator.newInstance().createSSLContext();
            sc.init(null, trustAllCerts, new SecureRandom());
            //LOGGER.warn("Trust all SSL cerificates has been installed");
            return sc;
        } catch (KeyManagementException e) {
            //LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("F", e);
        }
    }

    @POST
    public Response invokeJDBCMethod(String instanceType, String instanceID, JDBCMethod method) throws Exception {
        // debug
        /*System.err.println(System.currentTimeMillis() +
                " - instanceType: " + instanceType +
                ", instanceID: " + instanceID +
                ", methodName: " + method.getMethodName() +
                ", retInstanceID: " + method.getRetInstanceID());*/
        // debug end

        return client
                .target(url)
                .path(instanceType)
                .path(instanceID)
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(method, MediaType.APPLICATION_JSON));
    }
}
