package com.zz.okhttp.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


public class X509TrustManagerImpl implements X509TrustManager{
	@Override
	public void checkClientTrusted(X509Certificate[] chain, String authType)
	throws CertificateException {
	}
	@Override
	public void checkServerTrusted(X509Certificate[] chain, String authType) 
	throws CertificateException {
	}
	@Override
	public X509Certificate[] getAcceptedIssuers() {
		return new X509Certificate[0];
	}
}
