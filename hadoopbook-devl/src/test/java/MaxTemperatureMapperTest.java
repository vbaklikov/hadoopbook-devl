import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class MaxTemperatureMapperTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setUp(){
		 MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		 mapDriver = MapDriver.newMapDriver(mapper);
	}
	
	@Test
	public void parsesValidRecord() throws IOException, InterruptedException {
	    
		 
		 
		 Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
	                                  // Year ^^^^
	        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
	                              // Temperature ^^^^^
	    
	    mapDriver.withInput(new LongWritable(),value);
	    mapDriver.withOutput(new Text("1950"), new IntWritable(-11));
	    mapDriver.runTest();
	    
	 }

}
