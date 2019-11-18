package com.wjw19854.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class WordCountTest {

    public static boolean delDir(File dir){
        if (dir.isDirectory()){
            String[] children = dir.list();
            for (int i=0; i<children.length;i++) {
                boolean success = delDir(new File(dir, children[i]));
                if (!success)
                    return false;
            }

        }
        if (dir.delete()){
            System.out.println("目录已删除！！！");
            return true;
        }else {
            System.out.println("目录删除失败！！！！！");
        }
        return false;
    }
    @Before
    public void before(){
        delDir(new File("output/test"));
    }

    @Test
    public void testMapreduce() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output/test/t1"));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        assert job.waitForCompletion(true) == true;
    }

}