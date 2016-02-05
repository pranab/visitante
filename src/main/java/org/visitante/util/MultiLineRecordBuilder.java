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
 * Unless required by applicable law or agreed to in writing, softwarSessionSummarizere
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.visitante.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.chombo.util.Utility;

/**
 * Build record from multiple lines
 * @author pranab
 *
 */
public class MultiLineRecordBuilder {
	private String delim;
	private RecordBuilderStrategy builder;
	private String record;
	
	/**
	 * @param delim
	 */
	public MultiLineRecordBuilder(String delim){
		this.delim = delim;
	}
	
	/**
	 * @param regex
	 * @return
	 */
	public MultiLineRecordBuilder withPatternInFirstLine(String regex) {
		builder = new WithPatternInFirstLine(regex, delim);
		return this;
	}
	
	/**
	 * @param regex
	 * @return
	 */
	public MultiLineRecordBuilder withPatternInContinuationLines(String regex) {
		builder = new WithPatternInContinuationLines(regex, delim);
		return this;
	}

	/**
	 * @param line
	 */
	public void add(String line) {
		record = builder.add(line);
	}
	
	/**
	 * @return
	 */
	public boolean isRecordAvailable() {
		return null != record;
	}
	
	/**
	 * @return
	 */
	public String getRecord() {
		return record;
	}
	
	/**
	 * @return
	 */
	public String buildRecord() {
		record =  builder.getRecord();
		return record;
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static abstract class RecordBuilderStrategy {
		protected Pattern pattern;
		protected Matcher matcher;
		protected List<String> lines = new ArrayList<String>();
		protected String record;
		protected String delim;
		
		/**
		 * @param regex
		 * @param delim
		 */
		public RecordBuilderStrategy(String regex, String delim) {
			pattern = Pattern.compile(regex);
			this.delim = delim;
		}
		
		/**
		 * @param line
		 * @return
		 */
		public abstract String add(String line);
		
		/**
		 * @return
		 */
		public String getRecord() {
			record = null;
			if (!lines.isEmpty()) {
				record = Utility.join(lines, delim);
				lines.clear();
			}
			return record;
		}
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static class WithPatternInFirstLine extends RecordBuilderStrategy {
		
		/**
		 * @param regex
		 * @param delim
		 */
		public WithPatternInFirstLine(String regex, String delim) {
			super(regex, delim);
		}
		
		/* (non-Javadoc)
		 * @see org.visitante.util.MultiLineRecordBuilder.RecordBuilderStrategy#add(java.lang.String)
		 */
		public String add(String line) {
			record = null;
			matcher = pattern.matcher(line);
			if (matcher.find()) {
				//first line
				getRecord();
			} 
			lines.add(line);
			return record;
		}
	}
	/**
	 * @author pranab
	 *
	 */
	private static class WithPatternInContinuationLines extends RecordBuilderStrategy {
		
		/**
		 * @param regex
		 * @param delim
		 */
		public WithPatternInContinuationLines(String regex, String delim) {
			super(regex, delim);
		}
		
		/* (non-Javadoc)
		 * @see org.visitante.util.MultiLineRecordBuilder.RecordBuilderStrategy#add(java.lang.String)
		 */
		public String add(String line) {
			String record = null;
			matcher = pattern.matcher(line);
			if (!matcher.find()) {
				//first line
				getRecord();
			} 
			lines.add(line);
			return record;
		}
	}

}
