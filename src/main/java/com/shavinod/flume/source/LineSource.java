package com.shavinod.flume.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineSource extends AbstractSource implements Configurable, PollableSource {
	private static final Logger logger = LoggerFactory.getLogger(LineSource.class);
	private String myProp;
	private Thread tailThread;
	private BufferedReader br;

	public void configure(Context context) {
		// TODO Auto-generated method stub
		String myProp = context.getString("filepath", "default");
		logger.info("Path Property==============>" + myProp);
		this.myProp = myProp;

	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		ConcatRunner t = new ConcatRunner();
		tailThread = new Thread(t);
		tailThread.start();
	}

	public long getBackOffSleepIncrement() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Status process() throws EventDeliveryException {
		// TODO Auto-generated method stub
		return null;
	}

	private class ConcatRunner implements Runnable {

		public void run() {
			// TODO Auto-generated method stub
			Event e;
			String sCurrentLine;
			String finalFlumeString = null;
			try {
				br = new BufferedReader(new FileReader(myProp));

				while ((sCurrentLine = br.readLine()) != null) {
					System.out.println(sCurrentLine);
					finalFlumeString = finalFlumeString + sCurrentLine;
					e = EventBuilder.withBody(finalFlumeString, Charset.forName("UTF-8"));
					getChannelProcessor().processEvent(e);
					Thread.sleep(300);
				}

			} catch (Exception ex) {
				// TODO: handle exception
				System.out.println("Exception in Reading File" + ex.getMessage());        
			}
			
			try {
				if (br != null) 
					br.close();
				
			} catch (IOException ex) {
				// TODO: handle exception
				ex.printStackTrace();
			}

		}

	} // Concat Runner class

}
