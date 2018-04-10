package org.visitante.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pranab
 *
 */
public abstract class LogParser implements Serializable {
	protected String logFormatStd;
	protected Map<String, Object> fieldValues = new HashMap<String, Object>();

	/**
	 * @param line
	 * @return
	 */
	public abstract Map<String, Object> parse(String line);
	
	/**
	 * @param params
	 * @return
	 */
	public boolean contains(String... params) {
		boolean doesContain = true;
		for (String param : params) {
			if (!fieldValues.containsKey(param)) {
				doesContain = false;
				break;
			}
		}
		return doesContain;
	}
	
	/**
	 * @param params
	 * @return
	 */
	public Object[] getValues(String[] params) {
		Object[] values = new String[params.length];
		int i = 0;
		for (String param : params) {
			values[i++] = fieldValues.get(param);
		}
		return values;
	}

	/**
	 * @param params
	 * @return
	 */
	public String[] getStringValues(String[] params) {
		Object[] objValues = getValues(params);
		String[] values = new String[params.length];
		for (int i = 0; i < params.length; ++i) {
			values[i] = objValues[i].toString();
		}
		return values;
	}

	
	/**
	 * @param fieldName
	 * @return
	 */
	public abstract  String getFieldType(String fieldName);
}
