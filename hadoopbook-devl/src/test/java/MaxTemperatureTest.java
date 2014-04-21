import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;


public class MaxTemperatureTest {
	
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
	
	@Test
	public void testDriverWithMockConfiguration() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		
		Path input = new Path("input/ncdc/micro");
		Path output = new Path("output2");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{input.toString(),output.toString()});
		
		assertThat(exitCode,is(0));
		
		//checkOutput(conf,output);
		
	}
	
	private void checkOutput(Configuration conf, Path output) throws IOException {
	    FileSystem fs = FileSystem.getLocal(conf);
	    Path[] outputFiles = FileUtil.stat2Paths(
	        fs.listStatus(output, new OutputLogFilter()));
	    assertThat(outputFiles.length, is(2));
	    
	    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
	    BufferedReader expected = asBufferedReader(
	        getClass().getResourceAsStream("/expected.txt"));
	    String expectedLine;
	    while ((expectedLine = expected.readLine()) != null) {
	      assertThat(actual.readLine(), is(expectedLine));
	    }
	    assertThat(actual.readLine(), nullValue());
	    actual.close();
	    expected.close();
	  }
	  
	  private BufferedReader asBufferedReader(InputStream in) throws IOException {
	    return new BufferedReader(new InputStreamReader(in));
	  }

}
