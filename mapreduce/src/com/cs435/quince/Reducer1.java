package com.cs435.quince;

import java.io.IOException;
import java.util.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Math;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	    public boolean DEBUG = false;
		public ArrayList<HashMap<Double, ArrayList<Double> > > year_pm_reading_map = new ArrayList<HashMap<Double, ArrayList<Double> > >();
		

        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
        HashMap<Integer,Double> sizes = new HashMap<Integer,Double>();
        ArrayList<ArrayList<Double> > squaredErrors = new ArrayList<ArrayList<Double> >();         	
        String[] keys = key.toString().split("\t");
		String state = keys[0];
		String county = keys[1];
		
		if(year_pm_reading_map.size() == 0)
		{
			year_pm_reading_map =  initReadingMap(Main.numberOfFolds);
		}


		Calendar cal = new GregorianCalendar();
		double sumX = 0;
		double sumXY = 0;
		double sumY = 0;
		double sumX2 = 0;
		double size = 0;

		double sumX_1 = 0;
		double sumXY_1 = 0;
		double sumY_1 = 0;
		double sumX2_1 = 0;
		double size_1 = 0;

		double sumX_2 = 0;
		double sumXY_2 = 0;
		double sumY_2 = 0;
		double sumX2_2 = 0;
		double size_2 = 0;

		double sumX_3 = 0;
		double sumXY_3 = 0;
		double sumY_3 = 0;
		double sumX2_3 = 0;
		double size_3 = 0;

		double sumX_4 = 0;
		double sumXY_4 = 0;
		double sumY_4 = 0;
		double sumX2_4 = 0;
		double size_4 = 0;	
		double merge_size = 0;
		double merge_sumX = 0;
		double merge_sumXY = 0;
		double merge_sumY = 0;
		double merge_sumX2 = 0;				
		
		for(Text value : values){
			String[] tmpValues = value.toString().split("\t");
			String yearMonth = tmpValues[0];
			double year = Double.parseDouble(yearMonth.split("-")[0]);
			int month = Integer.parseInt(yearMonth.split("-")[1]);
			int day = Integer.parseInt(yearMonth.split("-")[2]);
			cal.setTime(new Date((int)year - 1900, month - 1, day - 1)); // Give your own date
			double dayOfYear = (double)cal.get(Calendar.DAY_OF_YEAR);
			
			year = year - 1990;
			year += dayOfYear / 365;
			double pmReading = Double.parseDouble(tmpValues[1]);
			Integer foldNumber = Integer.parseInt(tmpValues[2]);
			if(pmReading > 0){
				//calculate values for final prediction
				sumX += year;
				sumXY += pmReading * year;
				sumY += pmReading;
				sumX2 += year * year;
				size = size + 1.0;
				//calculate values for validation
				// if(foldNumber == 1)
				// {
				// 	year_pm_reading_map.put(year,pmReading);
				// }
				// else
				// {

				// }
		        switch (foldNumber) {
		            case 1: sumX_1 += year;
							sumXY_1 += pmReading * year;
							sumY_1 += pmReading;
							sumX2_1 += year * year;
							size_1 = size_1 + 1.0;
							addReadingToFold(0,year,pmReading);
		                     break;
		            case 2: sumX_2 += year;
							sumXY_2 += pmReading * year;
							sumY_2 += pmReading;
							sumX2_2 += year * year;
							size_2 = size_2 + 1.0;
							addReadingToFold(1,year,pmReading);
		                     break;
		            case 3: sumX_3 += year;
							sumXY_3 += pmReading * year;
							sumY_3 += pmReading;
							sumX2_3 += year * year;
							size_3 = size_3 + 1.0;
							addReadingToFold(2,year,pmReading);
		                     break;
		            case 4: sumX_4 += year;
							sumXY_4 += pmReading * year;
							sumY_4 += pmReading;
							sumX2_4 += year * year;
							size_4 = size_4 + 1.0;
							addReadingToFold(3,year,pmReading);
							break;
		            
		            default: 
		                     break;
		        }

			}
		}
		// a = n * sum(x, y) - sum(x) * sum(y)
		// 	n * sum(x^2) - sum(x)^2
		if(size > 1){
			try{
				double y = 0;
				double a = (size * sumXY - sumX * sumY) / (size * sumX2 - sumX * sumX);
		//int a = ((sumY * sumX2) - (sumX * sumXY)) / ((size * sumX2) - (sumX * sumX));

		// b = (1/n) (sum(y)  - sum(x))
				double b = (1 / size) * (sumY - a * sumX);
		//int b = ((size * sumXY) - (sumX * sumY)) / ((size * sumX2) - (sumX * sumX));
//System.out.println("sumX: " + sumX + " sumY: " + sumY + " sumXY: " + sumXY + " sumX2: " + sumX2 + " " + size);
				y = a * Main.predictionYear + b;
				y = ((int)(y * 100.0)) / 100.0;

				//keep track of the prediction versus the actual value
				for(Integer fold = 1; fold <= year_pm_reading_map.size();fold++)
				{
					for(Double year : year_pm_reading_map.get(fold-1).keySet())
					{
						switch(fold)
						{
							case 1: merge_size = size_2 + size_3 + size_4;
									merge_sumX = sumX_2 + sumX_3 + sumX_4;
									merge_sumXY = sumXY_2 + sumXY_3 + sumXY_4;
									merge_sumY = sumY_2 + sumY_3 + sumY_4;
									merge_sumX2 = sumX2_2 + sumX2_3 + sumXY_4;
									break;
							case 2: merge_size = size_1 + size_3 + size_4;
									merge_sumX = sumX_1 + sumX_3 + sumX_4;
									merge_sumXY = sumXY_1 + sumXY_3 + sumXY_4;
									merge_sumY = sumY_1 + sumY_3 + sumY_4;
									merge_sumX2 =sumX2_1 + sumX2_3 + sumXY_4;
									break;
							case 3: merge_size = size_1 + size_2 + size_4;
									merge_sumX = sumX_1 + sumX_2 + sumX_4;
									merge_sumXY = sumXY_1 + sumXY_2 + sumXY_4;
									merge_sumY = sumY_1 + sumY_2 + sumY_4;
									merge_sumX2 = sumX2_1 + sumX2_2 + sumX2_4;
									break;
							case 4: merge_size = size_1 + size_2 + size_4;
									merge_sumX = sumX_1 + sumX_2 + sumX_3;
									merge_sumXY = sumXY_1 + sumXY_2 + sumXY_3;
									merge_sumY = sumY_1 + sumY_2 + sumY_3;
									merge_sumX2 =	sumX2_1 + sumX2_2 + sumX2_3;						
									break;
						}

						Double aTrain = (merge_size * merge_sumXY - merge_sumX * merge_sumY) / (merge_size * merge_sumX2 - merge_sumX * merge_sumX);
						Double bTrain = (1 / merge_size) * (merge_sumY - aTrain * merge_sumX);
						sizes.put(fold,merge_size);
						Double yearPrediction =  aTrain * year + bTrain;
						yearPrediction = ((int)(yearPrediction * 100.0)) / 100.0;
						for(Double reading : year_pm_reading_map.get(fold-1).get(year))
						{
							if(squaredErrors.size() < Main.numberOfFolds)
							{
								squaredErrors = initSquaredError(Main.numberOfFolds);
							}
							if(DEBUG)
							{
								System.out.println("a:"+aTrain+" b:" +bTrain+"prediction:"+yearPrediction+"reading:"+reading+ "squaredError"+ Math.pow(yearPrediction-reading,2));
							}
							squaredErrors.get(fold-1).add(Math.pow(yearPrediction-reading,2));
						}
						
					}
				}

				Double averageRMSE = calculateAllRMSE(squaredErrors,sizes);

				context.write(new Text(state + " " + county), new Text(Double.toString(y) + "\t" + Double.toString(averageRMSE) + "\t" + Double.toString(size)));

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	public void addReadingToFold(Integer foldNum,Double year,Double reading)
	{
	
		//arraylist<hashmap<double, arraylist<double>>>
		ArrayList<Double> list = year_pm_reading_map.get(foldNum).get(year);
		if(list != null)
		{
			list.add(reading);
		}
		else
		{
			ArrayList<Double> newList = new ArrayList<Double>();
			newList.add(reading);
			year_pm_reading_map.get(foldNum).put(year,newList);
		}
	}
	public ArrayList<HashMap<Double, ArrayList<Double> > > initReadingMap(int numMaps)
	{
		ArrayList<HashMap<Double, ArrayList<Double> > > rMap = new ArrayList<HashMap<Double, ArrayList<Double> > >();
		HashMap<Double, ArrayList<Double> > temp = new HashMap<Double, ArrayList<Double> >();
		for( int i = 0; i < numMaps; i++)
		{
			rMap.add(temp);
		}
		return rMap;
	}
	public ArrayList<ArrayList<Double> > initSquaredError(int numLists)
	{
		ArrayList<ArrayList<Double> > result = new ArrayList<ArrayList<Double> >();
		ArrayList<Double> temp = new ArrayList<Double>();
		for(int i = 0; i <numLists; i++)
		{
			result.add(temp);
		}	
		return result;
	}

	public Double calculateRMSE(ArrayList<Double> squaredErrorList,Double n)
	{
		Double sumErrors = 0.0;
		for (Double error : squaredErrorList) {
			sumErrors += error;
		}
		if(DEBUG)
		{
			System.out.println("n in fold:"+n);
		}
		
		Double rmse = Math.sqrt(sumErrors / n);
		return rmse;
	}

	public Double calculateAllRMSE(ArrayList<ArrayList<Double> > squaredErrorsArray,HashMap<Integer,Double> sizesArr)
	{
		Integer fold = 1;
		Double overallSum = 0.0;
		Double averageRMSE = 0.0;
		for(ArrayList<Double> list : squaredErrorsArray)
		{
			Double currentRmse = calculateRMSE(list,sizesArr.get(fold));
			if(DEBUG)
			{
				System.out.println("one of the rmses:"+currentRmse);
			}
			
			overallSum += currentRmse;
			fold++;

		}
		averageRMSE = overallSum / squaredErrorsArray.size();
		return averageRMSE;
	}
}
