/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework;

import ucr.jppf.framework.lib.Job;

/**
 *
 * @author David
 */
public class Main {
    public static void main(String args[]) {
        String inputPath = args[0];
        String outputDir = inputPath;

        // Create job
        Job job = Job.getInstance("BigramCounter");

        // Setup MapReduce
        job.setMapperClass(BigramCountMapper.class);
        job.setCombinerClass(BigramCountReducer.class);
        job.setReducerClass(BigramCountReducer.class);
        job.setNumReduceTasks(4);
        job.setNumMapTasks(8);

        // Specify key / value
        job.setOutputKeyClass(Bigrama.class);
        job.setOutputValueClass(Integer.class);

        // Input
        job.setInputPath(inputPath);

        // Output
        job.setOutputPath(outputDir);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
