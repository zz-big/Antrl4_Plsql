package com.zz.okhttp;

import com.zz.okhttp.ssl.X509TrustManagerImpl;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.security.SecureRandom;


public class FastHttpClient {
    //
    public static Logger logger = LoggerFactory.getLogger(FastHttpClient.class);
    //
    public static final String VERSION = "1.6";
    //
    private static HttpClient httpClient = new HttpClient(getDefaultOkHttpClient());

    //
    private static OkHttpClient getDefaultOkHttpClient() {
        OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
        //
        final X509TrustManager trustManager = new X509TrustManagerImpl();
        SSLSocketFactory sslSocketFactory = null;
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[]{trustManager}, new SecureRandom());
            sslSocketFactory = sslContext.getSocketFactory();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return builder.sslSocketFactory(sslSocketFactory, trustManager).hostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        }).build();
    }

    //
    public static FastHttpClientBuilder newBuilder() {
        return new FastHttpClientBuilder(httpClient.getOkHttpClient());
    }

    //
    public static FastHttpClientBuilder newBuilder(OkHttpClient client) {
        return new FastHttpClientBuilder(client);
    }

    //
    public static GetBuilder get() {
        return httpClient.get();
    }

    //
    public static PostBuilder post() {
        return httpClient.post();
    }

    //
    public static PutBuilder put() {
        return httpClient.put();
    }
    //

    /**
     * @return the httpClient
     */
    public static HttpClient getHttpClient() {
        return httpClient;
    }

    /**
     * @param httpClient the httpClient to set
     */
    public static void setHttpClient(HttpClient httpClient) {
        FastHttpClient.httpClient = httpClient;
    }

    /**
     *
     */
    public static void cancelAll() {
        cancelAll(httpClient.getOkHttpClient());
    }

    /**
     * @param okHttpClient
     */
    public static void cancelAll(final OkHttpClient okHttpClient) {
        if (okHttpClient != null) {
            for (Call call : okHttpClient.dispatcher().queuedCalls()) {
                call.cancel();
            }
            for (Call call : okHttpClient.dispatcher().runningCalls()) {
                call.cancel();
            }
        }
    }

    /**
     * @param tag
     */
    public static void cancel(final Object tag) {
        cancel(httpClient.getOkHttpClient(), tag);
    }

    /**
     * @param tag
     */
    public static void cancel(final OkHttpClient okHttpClient, final Object tag) {
        if (okHttpClient != null && tag != null) {
            for (Call call : okHttpClient.dispatcher().queuedCalls()) {
                if (tag.equals(call.request().tag())) {
                    call.cancel();
                }
            }
            for (Call call : okHttpClient.dispatcher().runningCalls()) {
                if (tag.equals(call.request().tag())) {
                    call.cancel();
                }
            }
        }
    }
}
