/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.visitante.mr.bda;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author pranab
 */
public class ClassBoundary {
    private List<String> lines = new ArrayList<String>();
    private Histogram[] hists = new Histogram[2];
    
    public void process(String fileName){
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            String str;
            while ((str = in.readLine()) != null) {
                lines.add(str);
            }
            in.close();
            
            hists[0] = new Histogram();
            hists[1] = new Histogram();
            
            int totalCount = 0;
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (String line : lines){
                //System.out.println("processing:" + line);
                String[] items = line.split(",");
                if (items.length == 2){
                    int classVal = Integer.parseInt(items[0]);
                    int classCount = Integer.parseInt(items[1]);
                    Histogram hist = hists[classVal];
                    hist.setTotal(classCount);
                    totalCount += classCount;
                } else {
                    int classVal = Integer.parseInt(items[0]);
                    int value = Integer.parseInt(items[1]);
                    int count = Integer.parseInt(items[2]);
                    Histogram hist = hists[classVal];
                    hist.addCount(value, count);
                    
                    if (value < min) {
                        min = value;
                    }
                    if (value > max) {
                        max = value;
                    }
                }
            }
            
            for (Histogram hist : hists){
                hist.setSampleTotal(totalCount);
                hist.calculateProbability();
            }
            
            findBoundary(min, max);
        } catch (IOException e) {
        }        
    }
    
    private void findBoundary(int min, int max){
        Histogram hist0 = hists[0];
        Histogram hist1 = hists[1];
        
        for (int value = min; value <= max; ++value){
            //System.out.println("processing value:" + value);
            double h0 = hist0.getProbability(value) * hist0.getClassProb();
            double h1 = hist1.getProbability(value) * hist1.getClassProb();
            double postprob = h1 / (h0 + h1);
            System.out.println("" + value + "\t" + postprob);
        }
    }          
    
    
    private static class Histogram {
        private Map<Integer, Integer> countDist = new HashMap<Integer, Integer>();
        private Map<Integer, Double> probDensity = new HashMap<Integer, Double>();
        private int total;
        private int sampleTotal;
        private double classProb;

        /**
         * @param total the total to set
         */
        public void setTotal(int total) {
            this.total = total;
            System.out.println("total: " + total);
        }
        
        public void addCount(int value, int count) {
            countDist.put(value, count);
            //System.out.println("value: " + value + "count: " + count);
        }
        
        public void calculateProbability(){
            classProb = ((double)total) / sampleTotal;
            
            for (int value : countDist.keySet()){
                int count = countDist.get(value);
                double prob = ((double)count) / total;
                probDensity.put(value, prob);
                //System.out.println("value: " + value + "prob: " + prob);

            }
            
        }
        
        public double getProbability(int value){
            return probDensity.get(value);
        }
        
        /**
         * @param sampleTotal the sampleTotal to set
         */
        public void setSampleTotal(int sampleTotal) {
            this.sampleTotal = sampleTotal;
        }

        /**
         * @return the classProb
         */
        public double getClassProb() {
            return classProb;
        }
    }
    
    public static void  main(String[] args){
        new ClassBoundary().process(args[0]);
    }

}
