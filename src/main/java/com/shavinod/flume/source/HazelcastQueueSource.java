package com.shavinod.flume.source;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastQueueSource extends AbstractSource implements Configurable, PollableSource {

	public static final Logger logger = LoggerFactory.getLogger(HazelcastQueueSource.class);

	// Queue used
	private BlockingQueue<String> distributedQueue;

	// Hazelcast client
	private HazelcastInstance hazelcastClient;

	// properties required for hazelcast class
	private String queueName;
	private String serverIP;
	private String userName;
	private String userPwd;

	public long getBackOffSleepIncrement() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public synchronized void start() {
		// TODO Adding properties for queue
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName(userName).setPassword(userPwd);
		clientConfig.getNetworkConfig().addAddress(serverIP);
		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
		distributedQueue = hazelcastClient.getQueue(queueName);
	}

	@Override
	public synchronized void stop() {
		// TODO Shutdown hazelcast instance
		hazelcastClient.shutdown();
	}
	
	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		Status status = Status.READY;
		try {
			String msg = distributedQueue.poll(1000, TimeUnit.MILLISECONDS);
			if (msg == null) {
				return status.BACKOFF;
			}
			// Create event from the message
			Event event = EventBuilder.withBody(msg.getBytes());
			getChannelProcessor().processEvent(event);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("Error while reading message from queue",e);
			e.printStackTrace();
			status = status.BACKOFF;
		}

		return status;
	}

	public void configure(Context context) {
		// TODO Get the external properties needed for this class.
		queueName = context.getString("queueName", "default");
		serverIP = context.getString("serverIP", "localhost");
		userName = context.getString("userName", "default");
		userPwd = context.getString("userPwd", "default");

	}

}
