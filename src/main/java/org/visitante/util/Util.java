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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author pranab
 */
public class Util {
    public static List<String> splitTokens(String text) {
        return splitTokens(text, ",");
    }

    public static List<String> splitTokens(String text, String delim){
        List<String> tokens = new ArrayList<String>();
        String[] tokenArr = text.split(delim);
        for (String token :  tokenArr){
            tokens.add(token.trim());
        }
        return tokens;
    }
    
    public static String concatTokens(Collection<?> tokens){
         return concatTokens( tokens, ",");
    }

    public static String concatTokens(Collection<?> tokens, String delim){
       String text="";
       StringBuilder stBuilder = new StringBuilder();
       for (Object token : tokens){
           stBuilder.append(token).append(delim);
       }
       text = stBuilder.substring(0, stBuilder.length() - 1);
       return text;
    }
    
    public static <T> List<T> getSubList(List<T> list, int  start, int end){
         List<T> subList = new ArrayList<T>();
         int len = list.size();
         if (start >= 0 && end > start && end <= len ){
             for (int i = start; i < end; ++i){
                 subList.add(list.get(i));
             }
         }
         return subList;
    }

    public static <T> List<T> getSubList(List<T> list, List<Integer>  selectors){
         List<T> subList = new ArrayList<T>();
         int len = list.size();
         for (int i : selectors){
             if (i >= len){
                 throw new IllegalArgumentException("Index out of range of source list");
             }
             subList.add(list.get(i));
         }
         return subList;

    }
}

