package com.shavinod.flume.intercept;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHostInterceptor implements Interceptor {
	private static final Logger logger = LoggerFactory.getLogger(CustomHostInterceptor.class);
	private String hostValue;

	// supplied through the flume configuration using key "hostHeader"
	private String hostHeader;

	// create a constructor of the class
	public CustomHostInterceptor(String hostHeader) {
		// TODO Auto-generated constructor stub
		this.hostHeader = hostHeader;
	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void initialize() {
		// TODO Auto-generated method stub
		// At intercept startup
		try {
			hostValue = InetAddress.getLocalHost().getHostName();
			logger.info("hostValue ===================> {}", hostValue);
		} catch (UnknownHostException e) {
			// TODO: handle exception
			throw new FlumeException("Cannot get Hostname", e);
		}

	}

	public Event intercept(Event event) {
		// TODO Auto-generated method stub
		// This is the event body
		String body = new String(event.getBody());
		logger.info("event body ===================> {}", body);

		// These are event headers
		Map<String, String> headers = event.getHeaders();

		// Enrich header with hostname and hostvalue
		headers.put(hostHeader, hostValue);

		// Let the enriched event go
		return event;
	}

	public List<Event> intercept(List<Event> events) {
		// TODO Auto-generated method stub
		List<Event> interceptedEvents = new ArrayList<Event>(events.size());

		// loop through the list
		for (Event event : events) {

			Event interceptedEvent = intercept(event);
			interceptedEvents.add(interceptedEvent);
		}

		return interceptedEvents;
	}

	public static class Builder implements Interceptor.Builder {

		private String hostHeader;

		public void configure(Context context) {
			// TODO Auto-generated method stub
			// read property from flume conf
			hostHeader = context.getString("hostHeader");
		}

		public Interceptor build() {
			// TODO Auto-generated method stub
			return new CustomHostInterceptor(hostHeader);
		}
	}

}
