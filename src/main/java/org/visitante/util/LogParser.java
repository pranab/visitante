/*
 * visitante: Web analytic using Hadoop Map Reduce
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.visitante.util;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class LogParser implements Serializable {
	private String logFormatStd;
	private String sessionIdName;
	private String userIdName;
	private DateFormat sourceDateTimeFormat;
	private DateFormat dateTimeFormat;
	private Map<String, Object> fieldValues = new HashMap<String, Object>();
	
	public static final String LOG_FORMAT_NCSA = "NCSA";
	public static final String LOG_FORMAT_W3C = "W3C";
	public static final String CLIENT_IP_ADDRESS = "clientIpAddress";
	public static final String CLIENT_ID = "clientId";
	public static final String USER_NAME = "userName";
	public static final String DATE_TIME = "dateTime";
	public static final String HTTP_METHOD = "httpMethod";
	public static final String REQUEST_URL = "requestUrl";
	public static final String HTTP_VERSION = "httpVersion";
	public static final String STATUS_CODE = "statusCode";
	public static final String NUM_BYTES = "numBytes";
	public static final String REFERRER = "referrer";
	public static final String USER_AGENT = "userAgent";
	public static final String COOKIE = "cookie";
	public static final String SESSION_ID = "sessionId";
	public static final String USER_ID = "userId";
	
	/**
	 * @param logFormatStd
	 * @param sessionIdName
	 * @param userIdName
	 */
	public LogParser(String logFormatStd, String sessionIdName, String userIdName, String dateTimeFormatStr) {
		this.logFormatStd = logFormatStd;
		this.sessionIdName = sessionIdName;
		this.userIdName = userIdName;
		String sourceDateTimeFormatStr = null;
		if (logFormatStd.equals(LOG_FORMAT_NCSA)) {
			sourceDateTimeFormatStr = "dd/MMM/yyyy:HH:MM:SS Z";
		} else if (logFormatStd.equals(LOG_FORMAT_W3C)) {
			sourceDateTimeFormatStr = "yyyy-MM-dd HH:MM:SS";
		}
		sourceDateTimeFormat = new SimpleDateFormat(sourceDateTimeFormatStr);
		if (!dateTimeFormatStr.equals(BasicUtils.EPOCH_TIME)){
			dateTimeFormat = new SimpleDateFormat(dateTimeFormatStr);
		}
	}
	
	/**
	 * @param logFormatStd
	 * @param sessionIdName
	 * @param userIdName
	 */
	public LogParser(String logFormatStd, String sessionIdName, String dateTimeFormatStr) {
		this(logFormatStd, sessionIdName, null, dateTimeFormatStr);
	}
	
	/**
	 * @param line
	 * @return
	 */
	public Map<String, Object> parse(String line) {
		fieldValues.clear();
		if (logFormatStd.equals(LOG_FORMAT_NCSA)) {
			parseNCSA(line);
		} else if (logFormatStd.equals(LOG_FORMAT_W3C)) {
			parseW3C(line);
		} else {
			throw new IllegalArgumentException("invalid log format");
		}
		
		return fieldValues;
	}
	
	/**
	 * @param line
	 * @return
	 */
	public void parseNCSA(String line) {
		String[] items = line.split("\\s+");
		fieldValues.put(CLIENT_IP_ADDRESS, items[0]);
		fieldValues.put(CLIENT_ID, items[1]);
		fieldValues.put(USER_NAME, items[2]);
		
		String dateTimeStr = BasicUtils.slice(items[3] + " " + items[4], 1);
		Date date = null;
		try {
			date = sourceDateTimeFormat.parse(dateTimeStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		fieldValues.put(DATE_TIME, date);
		
		fieldValues.put(HTTP_METHOD, BasicUtils.rightSlice(items[5], 1));
		fieldValues.put(REQUEST_URL, items[6]);
		fieldValues.put(HTTP_VERSION, BasicUtils.leftSlice(items[7], 1));
		fieldValues.put(STATUS_CODE, items[8]);
		fieldValues.put(NUM_BYTES, items[9]);
		
		String[] quItems = line.split("\"");
		fieldValues.put(REFERRER, quItems[3]);
		fieldValues.put(USER_AGENT, quItems[5]);
		String[] cookieItems = quItems[7].split(";");
		for (String cookieItem : cookieItems) {
			String[] nameValue = cookieItem.split("=");
			if (nameValue[0].equals(sessionIdName)) {
				fieldValues.put(SESSION_ID, nameValue[1]);
			} else if (nameValue[0].equals(userIdName)) {
				fieldValues.put(USER_ID, nameValue[1]);
			}
		}
	}
	
	/**
	 * @param line
	 * @return
	 */
	public void parseW3C(String line) {
	}
	
	/**
	 * @param params
	 * @return
	 */
	public String[] getStringValues(String[] params) {
		String[] values = new String[params.length];
		
		return values;
	}

	/**
	 * @param params
	 * @return
	 */
	public String[] getStringValues(List<String> params) {
		String[] values = new String[params.size()];
		
		return values;
	}
}
