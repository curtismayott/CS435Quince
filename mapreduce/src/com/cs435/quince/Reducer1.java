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

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
        ArrayList<HashMap<Double, Double> > year_pm_reading_map = new ArrayList<HashMap<Double,Double> >();
        HashMap<Double, Double> prediction_actual_map = new HashMap<Double,Double>();         	
        String[] keys = key.toString().split("\t");
		String state = keys[0];
		String county = keys[1];
		


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
							year_pm_reading_map.get(0).put(year,pmReading);
		                     break;
		            case 2: sumX_2 += year;
							sumXY_2 += pmReading * year;
							sumY_2 += pmReading;
							sumX2_2 += year * year;
							size_2 = size_1 + 1.0;
							year_pm_reading_map.get(1).put(year,pmReading);
		                     break;
		            case 3: sumX_3 += year;
							sumXY_3 += pmReading * year;
							sumY_3 += pmReading;
							sumX2_3 += year * year;
							size_3 = size_3 + 1.0;
							year_pm_reading_map.get(2).put(year,pmReading);
		                     break;
		            case 4: sumX_4 += year;
							sumXY_4 += pmReading * year;
							sumY_4 += pmReading;
							sumX2_4 += year * year;
							size_4 = size_1 + 1.0;
							year_pm_reading_map.get(3).put(year,pmReading);
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
				for(Integer i = 0; i < year_pm_reading_map.size();i++)
				{
					for(Double year : year_pm_reading_map.get(i).keySet())
					{
						Double yearPrediction = a * year + b;
						prediction_actual_map.put(yearPrediction,year_pm_reading_map.get(i).get(year));
					}
				}

				//context.write(new Text(state + " " + county), new Text(Double.toString(y)));

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
