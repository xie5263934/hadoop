package com.sheldon.test;

import com.test.sheldon.MaxTemperatureDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;



public class MaxTemperatureDriverTest{
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        conf.setInt("mapreduce.task.io.sort.mb",1);
        Path input = new Path("/home/sheldon/ncdc/sample.txt");
        Path output = new Path("/home/sheldon/ncdc/output.txt");
        FileSystem fileSystem = FileSystem.getLocal(conf);
        fileSystem.delete(output,true);
        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        int exitCode = driver.run(new String[]{input.toString(),output.toString()});
        Assert.assertEquals(exitCode,0);
    }
}
