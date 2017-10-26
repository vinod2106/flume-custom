package com.shavinod.flume.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.shavinod.flume.intercept.CustomHostInterceptor;

public class TestCustomHostInterceptor {
	private static final Logger logger = LoggerFactory.getLogger(TestCustomHostInterceptor.class);
	private static final String eBody = "Event body :";
	private static final String TAG_HADOOP = "#Hadoop";
	private Map<String, String> headers = new HashMap<String, String>();

	
	@Test
	public void testCustomIntercept()
			throws ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException {

		logger.info("Starting test case");
		Event event;
		Interceptor.Builder builder = CustomHostInterceptor.Builder.class.newInstance();

		Context ctx = new Context();
		ctx.put("hostHeader", "HostName");

		builder.configure(ctx);
		Interceptor interceptor = builder.build();
		for (int i = 0; i < 5; i++) {
		      headers.put(TAG_HADOOP, Integer.valueOf(i * 10).toString());
		      event = EventBuilder.withBody((eBody+i).getBytes(), headers);
		      interceptor.intercept(event);
		      Thread.sleep(100);
		    }
		
	}

	
}