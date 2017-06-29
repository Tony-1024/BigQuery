package com.example.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Calculate the mean and median length of GitHub commit messages via BigQuery.
 * @author Tony Liu
 *
 */
public class MyQuery {
	// key (message length), value (duplicate times)
	private static TreeMap<Integer, Integer> valTimesMap=new TreeMap<>();
	/**
	 * @param currentDate: the query date
	 * @param lastTime: the last time stamp
	 * @return
	 * @throws Exception
	 */
	public static TreeMap<Integer, Integer> callBigQuery(String currentDate, long lastTime)  throws Exception{
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(
				"select length(JSON_EXTRACT(payload, '$.commits[0].message')) as msglen FROM [githubarchive:day."
						+ currentDate + "] where type='PushEvent' and TIMESTAMP_TO_MSEC(created_at)>" + lastTime)
				.setUseLegacySql(true).build();
	    // Create a job ID
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
	    queryJob = queryJob.waitFor();
	    // check error cases
	    if (queryJob == null) {
	      throw new RuntimeException("Job not exists");
	    } else if (queryJob.getStatus().getError() != null) {
	    	throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }

	    // Get the results
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    QueryResult result = response.getResult();
	    
	    // record result into map
		if (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
				String mlenStr = (String) row.get(0).getValue();
				if(mlenStr!=null){
					Integer msgLen = Integer.parseInt(mlenStr);
					valTimesMap.put(msgLen, valTimesMap.getOrDefault(msgLen, 0) + 1);
				}
			}
		}
	    return valTimesMap;
	}
}