package com.zz.okhttp;

import okhttp3.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public class Response {
	//
	private okhttp3.Response response;
	
	/**
	 * 
	 * @param response
	 */
	public Response(okhttp3.Response response){
		this.response=response;
	}
	
	/**
	 * 
	 * @return
	 */
	public Request request() {
		return response.request();
	}

	/**
	 * 
	 * @return
	 */
	public Protocol protocol() {
		return response.protocol();
	}

	/**
	 * 
	 * @return
	 */
	public int code() {
		return response.code();
	}

	/**
	 * 
	 * @return
	 */
	public boolean isSuccessful() {
		return response.isSuccessful();
	}

	/**
	 * 
	 * @return
	 */
	public String message() {
		return response.message();
	}

	/**
	 * Returns the TLS handshake of the connection that carried this response,
	 * or null if the response was received without TLS.
	 */
	public Handshake handshake() {
		return response.handshake();
	}

	public List<String> headers(String name) {
		return response.headers(name);
	}

	public String header(String name) {
		return response.header(name, null);
	}

	public String header(String name, String defaultValue) {
		return response.header(name, defaultValue);
	}

	public Headers headers() {
		return response.headers();
	}

	/**
	 * Peeks up to {@code byteCount} bytes from the response body and returns
	 * them as a new response body. If fewer than {@code byteCount} bytes are in
	 * the response body, the full response body is returned. If more than
	 * {@code byteCount} bytes are in the response body, the returned value will
	 * be truncated to {@code byteCount} bytes.
	 *
	 * <p>
	 * It is an error to call this method after the body has been consumed.
	 *
	 * <p>
	 * <strong>Warning:</strong> this method loads the requested bytes into
	 * memory. Most applications should set a modest limit on {@code byteCount},
	 * such as 1 MiB.
	 */
	public ResponseBody peekBody(long byteCount) throws IOException {
		return response.peekBody(byteCount);
	}

	/**
	 * Never {@code null}, must be closed after consumption, can be consumed
	 * only once.
	 */
	public ResponseBody body() {
		return response.body();
	}

	//
	/**
	 * Returns the response as a string decoded with the charset of the
	 * Content-Type header. If that header is either absent or lacks a charset,
	 * this will attempt to decode the response body as UTF-8.
	 */
	public final String string() throws IOException {
		return body().string();
	}
	//
	/**
	 * Returns the response as a string decoded with the charset of the
	 * Content-Type header. If that header is either absent or lacks a charset,
	 * this will attempt to decode the response body as UTF-8.
	 */
	public final String string(String charset) throws IOException {
		return new String(body().bytes(),charset);
	}
	//
	public final byte[] bytes() throws IOException {
		return body().bytes();
	}
	//
	public final InputStream byteStream() {
	    return body().source().inputStream();
	}
	//
	public okhttp3.Response getResponse(){
		return response;
	}
	//
}
