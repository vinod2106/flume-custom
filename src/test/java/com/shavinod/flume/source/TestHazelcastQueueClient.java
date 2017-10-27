package com.shavinod.flume.source;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.BlockingQueue;


public class TestHazelcastQueueClient {
	public static final Logger logger = LoggerFactory.getLogger(TestHazelcastQueueClient.class);
	private HazelcastInstance hazelcastClient;
	private BlockingQueue<String> distributedQueue;

	private void init(String queueName){
		// TODO Auto-generated method stub
		logger.info("queueName =======> " + queueName);
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
		clientConfig.getNetworkConfig().addAddress("120.0.0.1");
		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
		distributedQueue = hazelcastClient.getQueue(queueName);
	}

	@Test
	public void test() {
		// Send the messages to queue
		TestHazelcastQueueClient hazelcastQueueClient = new TestHazelcastQueueClient();
		hazelcastQueueClient.init("queue");
		hazelcastQueueClient.sendTextMessages(Integer.parseInt("5"));
	}

	private void sendTextMessages(int numOfTimes) {
		// TODO Auto-generated method stub

		for (int i = 0; i < numOfTimes; i++) {
			try {
				distributedQueue.add("msg#" + i);
			} catch (Exception e) {
				// TODO: handle exception
				logger.error("Error in the send msg method " + e);
				e.printStackTrace();
			}

		}

	}

}
