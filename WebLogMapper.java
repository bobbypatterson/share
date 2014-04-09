/*
 * Copyright (c) 2014, Optimetrum LLC. All Rights Reserved.
 * 
 * This is a mapper class used to parse and filter web log entries. 
 */

package com.optimetrum.share;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	// Use tab delimiter for output and input files
	private static final String delim = "\t";
	
	private ArrayList<String> urlFilter;
	private ArrayList<String> botFilter;
	private ArrayList<String> ipFilter;
	
	private Text logObject = new Text();

	protected void setup(Context context) {

		// Set up URL filter for JavaScript, images, and CSS
		urlFilter = new ArrayList<String>(3);
		urlFilter.add(".js");
		urlFilter.add(".jpg");
		urlFilter.add(".css");
		
		// Set up filter for known bots such as Google
		botFilter = new ArrayList<String>(1);
		botFilter.add("Googlebot");
		
		// Establish list of IPs for filtering
		ipFilter = new ArrayList<String>(1);
		ipFilter.add("75.72.48.19");
		
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Default if no campaign set in URL query string
		String campaign = "none";
	
		// Split the input line based on delimiter 
		String[] allFields = value.toString().split(delim);

		// Split out fields that we need to determine whether or not to keep this record
		String[] urlFields = allFields[6].split(" ");
		String url = urlFields[1];
		String userAgent = allFields[10];
		String ip = allFields[2];

		/* call filter methods to determine if we should keep this record
		 * if so, continue to process the log record and write to output
		 */
		if (filterUrl(url) && filterBot(userAgent) && filterIp(ip)) {
			
			String visitor = getVisitor(allFields);
			
			// Get rid of extraneous characters in timestamp
			String[] timeFields = allFields[5].split(" ");
			String timestamp = timeFields[0].replaceAll("\\[", "");

			// Save campaign parameter if it exists and keep base part of URL
			String[] urlParm = url.toString().split("[\\?\\=]");
			if (urlParm.length > 2) {
				campaign = urlParm[2];
				url = urlParm[0];
			}
			
			String responseCode = allFields[7];
			String referer = allFields[9];

			// Concatenate fields to build output record
			logObject.set(visitor + delim + ip + delim + timestamp + delim + url + delim + campaign + delim + responseCode + delim + referer + delim + userAgent);

			// write out record with null key since this is a map-only job
			context.write(NullWritable.get(), logObject);
		}

	}
	
	public String getVisitor (String[] logline) {
		
		// if new cookie wasn't set for this request, use existing cookie value	for visitor id  
		if (logline[0].equalsIgnoreCase("-")) {
			String cookieNameVal = logline[1];
			String[] cookieVal = cookieNameVal.split("=");
			return cookieVal[1];
		} else {
			return logline[0];
		}
	}
	
	public boolean filterUrl (String url) {
		
		// Check URL against filter rules and return false to discard this record
		for (String s : urlFilter) {
			if(url.contains(s)) {
				return false;
			}
		}
		
		// if record hasn't been rejected by above rules, return true
		return true;
	}
	
	public boolean filterBot (String useragent) {
		
		// Check user agent against filter rules and return false to discard this record
		for (String s : botFilter) {
			if(useragent.contains(s)){
				return false;
			}
		}
		
		// if record hasn't been rejected by above rules, return true
		return true;
	}
	
	public boolean filterIp (String ip) {
		
		// Check IP address against filter rules and return false to discard this record
		for (String s : ipFilter) {
			if(ip.equalsIgnoreCase(s)) {
				return false;
			}
		}
		
		// if record hasn't been rejected by above rules, return true
		return true;
	}
}
