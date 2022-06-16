package com.zz.okhttp.callback;

import com.zz.okhttp.Response;
import okhttp3.Call;


public abstract class Callback{
	//
	public abstract void onFailure(Call call,Exception e,String id);
	//
	public abstract void onResponse(Call call, Response response, String id);
}