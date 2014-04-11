import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class MaxTemperatureMapperTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	
	@Test
	public void parsesValidRecord() throws IOException, InterruptedException {
	    
		 
		 
		 Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
	                                  // Year ^^^^
	        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
	                              // Temperature ^^^^^
		mapDriver = MapDriver.newMapDriver(new MaxTemperatureMapper());
	    mapDriver.withInput(new LongWritable(),value);
	    mapDriver.withOutput(new Text("1950"), new IntWritable(-11));
	    mapDriver.runTest();
	    
	 }
	
	@Test
	public void testIgnoreMissingTemperatureRecord(){
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                			 // Year ^^^^
"99999V0203201N00261220001CN9999999N9+99991+99999999999");
            		  // Temperature ^^^^^
		mapDriver = MapDriver.newMapDriver(new MaxTemperatureMapper());
		mapDriver.withInput(new LongWritable(),value);
		mapDriver.runTest();
		
	}
	
	@Test
	public void testReturnsMaximumIntegerInValues(){
		ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver = 
				ReduceDriver.newReduceDriver(new MaxTemperatureReducer());
		reduceDriver.withInputKey(new Text("1950"));
		reduceDriver.withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)));
		reduceDriver.withOutput(new Text("1950"), new IntWritable(10));
		reduceDriver.runTest();
		
	}

}
