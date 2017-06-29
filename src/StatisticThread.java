package com.example.bigquery;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Thread for statistic from BigQuery.
 * @author Tony Liu
 *
 */
public class StatisticThread extends Thread {
	private long INITIAL_TIME_SPAN = 6 * 60 * 60 * 1000; // first time span is 6
	private long PERIOD_TIME = 2 * 60 * 1000; // Time for each cycle is 6 hour
	private long lastTime;
	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

	public void run() {
		try {
			while (true) {
				// get the current date
				Date currentDate = new Date();
				String currentDateStr = formatter.format(currentDate);
				long currentTime = System.currentTimeMillis();
				// for the first round, the time span is INITIAL_TIME_SPAN
				if (lastTime == 0)
					lastTime = currentTime - INITIAL_TIME_SPAN;
				TreeMap<Integer, Integer> map = MyQuery.callBigQuery(currentDateStr, lastTime);
				int count = 0;
				double average = 0;
				for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
					for (int i = 0; i < entry.getValue(); i++) {
						count++;
						average = average * (count - 1) / count + (double) entry.getKey() / count;
					}
				}
				// Calculate the mean value
				int mean = 0, k = 0;
				if ((count & 1) == 1) // when length is odd, mean is the middle element
				{
					for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
						if (k + entry.getValue() >= count / 2 + 1) {
							mean = entry.getKey();
							break;
						}
						k += entry.getValue();
					}
				} else { // when length is even, mean is the average of middle two elements
					Iterator<Entry<Integer, Integer>> it = map.entrySet().iterator();
					while (it.hasNext()) {
						Entry<Integer, Integer> entry = it.next();
						if (k + entry.getValue() > count / 2) { // the middle two elements are the same
							mean = entry.getKey();
							break;
						} else if (k + entry.getValue() == count / 2) { // the middle two elements are different
							mean = (entry.getKey() + it.next().getKey()) / 2;
							break;
						}
						k += entry.getValue();
					}
				}
				System.out.println("Average:" + average + ", Mean:" + mean + ", Time:" + currentDate);
				lastTime = currentTime; // update the time stamp
				Thread.sleep(PERIOD_TIME);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
