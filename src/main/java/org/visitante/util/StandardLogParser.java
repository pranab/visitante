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

import org.chombo.util.BaseAttribute;
import org.chombo.util.BasicUtils;

/**
 * @author pranab
 *
 */
public class StandardLogParser extends LogParser {
	private String sessionIdName;
	private String userIdName;
	protected DateFormat sourceDateTimeFormat;
	protected DateFormat dateTimeFormat;
	
	public static final String CLIENT_IP_ADDRESS = "clientIpAddress";
	public static final String CLIENT_ID = "clientId";
	public static final String USER_NAME = "userName";
	public static final String DATE_TIME = "dateTime";
	public static final String HTTP_METHOD = "httpMethod";
	public static final String REQUEST_URL = "requestUrl";
	public static final String HTTP_VERSION = "httpVersion";
	public static final String STATUS_CODE = "statusCode";
	public static final String CS_NUM_BYTES = "csNumBytes";
	public static final String REFERRER = "referrer";
	public static final String USER_AGENT = "userAgent";
	public static final String COOKIE = "cookie";
	public static final String SESSION_ID = "sessionId";
	public static final String USER_ID = "userId";
	
	public static final String DATE = "date";
	public static final String TIME = "time";
	public static final String SERVER_IP_ADDRESS = "serverIpAddress";
	public static final String REQUEST_URL_QUERY = "requestUrlQuery";
	public static final String SC_NUM_BYTES = "scNumBytes";
	public static final String TIME_TAKEN = "timeTaken";

	
	public static final String TIME_ON_PAGE = "timeOnPage";
	public static final String DATE_TIME_EPOCH = "dateTimeEpoch";
	
	
	/**
	 * 
	 */
	public StandardLogParser() {
	}
	
	/**
	 * @param logFormatStd
	 * @param sessionIdName
	 * @param userIdName
	 */
	public StandardLogParser(String logFormatStd, String sessionIdName, String userIdName, String dateTimeFormatStr) {
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
	public StandardLogParser(String logFormatStd, String sessionIdName, String dateTimeFormatStr) {
		this(logFormatStd, sessionIdName, null, dateTimeFormatStr);
	}
	
	/* (non-Javadoc)
	 * @see org.visitante.util.LogParser#parse(java.lang.String)
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
	private void parseNCSA(String line) {
		String[] items = BasicUtils.splitWithEmbeddedDelim(line, "\\s+", "\""," ", "+");
		int i = 0;
		fieldValues.put(CLIENT_IP_ADDRESS, items[i++]);
		fieldValues.put(CLIENT_ID, items[i++]);
		fieldValues.put(USER_NAME, items[i++]);
		
		Date date = null;
		try {
			date = sourceDateTimeFormat.parse(items[i++]);
		} catch (ParseException e) {
			throw new IllegalArgumentException("failed date parsing " + e.getMessage());
		}
		fieldValues.put(DATE_TIME, date);
		
		fieldValues.put(HTTP_METHOD, items[i++]);
		fieldValues.put(REQUEST_URL, items[i++]);
		fieldValues.put(HTTP_VERSION, items[i++]);
		fieldValues.put(STATUS_CODE, items[8]);
		fieldValues.put(CS_NUM_BYTES, Integer.parseInt(items[i++]));
		fieldValues.put(REFERRER, items[i++]);
		fieldValues.put(USER_AGENT, items[i++]);
		
		String[] cookieItems = items[i++].split(";");
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
	private void parseW3C(String line) {
		String[] tmpItems = line.split("\\s+");
		String dateTime = tmpItems[0] + " " + tmpItems[1];
		String[] items = new String[tmpItems.length -1];
		int i = 0;
		int j = 2;
		items[i++] = dateTime;
		for ( ; j < tmpItems.length; ++i, ++j) {
			items[i] = tmpItems[j];
		}
		
		Date date = null;
		i = 0;
		try {
			date = sourceDateTimeFormat.parse(items[i++]);
		} catch (ParseException e) {
			throw new IllegalArgumentException("failed date parsing " + e.getMessage());
		}
		fieldValues.put(DATE_TIME, date);
		fieldValues.put(CLIENT_IP_ADDRESS, items[i++]);
		fieldValues.put(USER_NAME, items[i++]);
		fieldValues.put(SERVER_IP_ADDRESS, items[i++]);
		fieldValues.put(HTTP_METHOD, items[i++]);
		fieldValues.put(REQUEST_URL, items[i++]);
		fieldValues.put(REQUEST_URL_QUERY, items[i++]);
		fieldValues.put(STATUS_CODE, items[i++]);
		fieldValues.put(SC_NUM_BYTES, Integer.parseInt(items[i++]));
		fieldValues.put(CS_NUM_BYTES, Integer.parseInt(items[i++]));
		fieldValues.put(TIME_TAKEN, Integer.parseInt(items[i++]));
		fieldValues.put(HTTP_VERSION, items[i++]);
		fieldValues.put(USER_AGENT, items[i++]);
		
		String[] cookieItems = items[i++].split(";+");
		for (String cookieItem : cookieItems) {
			String[] nameValue = cookieItem.split("=");
			if (nameValue[0].equals(sessionIdName)) {
				fieldValues.put(SESSION_ID, nameValue[1]);
			} else if (nameValue[0].equals(userIdName)) {
				fieldValues.put(USER_ID, nameValue[1]);
			}
		}
		
		fieldValues.put(REFERRER, items[i++]);
	}
	
	/* (non-Javadoc)
	 * @see org.visitante.util.LogParser#getFieldType(java.lang.String)
	 */
	public  String getFieldType(String fieldName) {
		String fieldType = BaseAttribute.DATA_TYPE_STRING;
		if (fieldName.equals(DATE_TIME)) {
			fieldType = BaseAttribute.DATA_TYPE_DATE;
		} else if (fieldName.equals(DATE_TIME_EPOCH)) {
			fieldType = BaseAttribute.DATA_TYPE_LONG;
		} else if (fieldName.equals(TIME_ON_PAGE)) {
			fieldType = BaseAttribute.DATA_TYPE_INT;
		} else if (fieldName.equals(CS_NUM_BYTES)) {
			fieldType = BaseAttribute.DATA_TYPE_INT;
		} else if (fieldName.equals(SC_NUM_BYTES)) {
			fieldType = BaseAttribute.DATA_TYPE_INT;
		} else if (fieldName.equals(TIME_TAKEN)) {
			fieldType = BaseAttribute.DATA_TYPE_INT;
		}
		
		return fieldType;
	}

}
