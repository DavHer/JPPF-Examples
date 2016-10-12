/*
 * JPPF.
 * Copyright (C) 2005-2016 JPPF Team.
 * http://www.jppf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jppf.application.template;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import org.jppf.client.*;
import org.jppf.node.protocol.Task;

/**
 * This is a template JPPF application runner. It is fully commented and is
 * designed to be used as a starting point to write an application using JPPF.
 *
 * @author Laurent Cohen
 */
public class TemplateApplicationRunner {

    public static final int TAREAS = 4;

    public static void main(final String... args) {

        // create the JPPFClient. This constructor call causes JPPF to read the configuration file
        // and connect with one or multiple JPPF drivers.
        try (JPPFClient jppfClient = new JPPFClient()) {

            // create a runner instance.
            TemplateApplicationRunner runner = new TemplateApplicationRunner();

            // create and execute a blocking job
            runner.executeBlockingJob(jppfClient);

            // create and execute a non-blocking job
            //runner.executeNonBlockingJob(jppfClient);
            // create and execute 3 jobs concurrently
            //runner.executeMultipleConcurrentJobs(jppfClient, 3);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a JPPF job that can be submitted for execution.
     *
     * @param jobName an arbitrary, human-readable name given to the job.
     * @return an instance of the {@link org.jppf.client.JPPFJob JPPFJob} class.
     * @throws Exception if an error occurs while creating the job or adding
     * tasks.
     */
    public JPPFJob createJob(final String jobName) throws Exception {
        File file;
        FileChannel fileChannel;
        long size;
        long from = 0, to = 0;
        Task<?> task;

        // create a JPPF job
        JPPFJob job = new JPPFJob();
        // give this job a readable name that we can use to monitor and manage it.
        job.setName(jobName);

        file = new File("numeros.txt");
        fileChannel = new RandomAccessFile(file, "r").getChannel();
        size = fileChannel.size();
        fileChannel.close();
        
        for(int i = 0; i < TAREAS; i++){
            to = from + size/TAREAS;
            System.out.println(from + ":" + to);
            task = job.add(new TemplateJPPFTask(from, to));
            task.setId(jobName + " - task " + i);
            from = to;
        }

        return job;
    }

    /**
     * Execute a job in blocking mode. The application will be blocked until the
     * job execution is complete.
     *
     * @param jppfClient the {@link JPPFClient} instance which submits the job
     * for execution.
     * @throws Exception if an error occurs while executing the job.
     */
    public void executeBlockingJob(final JPPFClient jppfClient) throws Exception {
        // Create a job
        JPPFJob job = createJob("Template blocking job");

        // set the job in blocking mode.
        job.setBlocking(true);

        // Submit the job and wait until the results are returned.
        // The results are returned as a list of Task<?> instances,
        // in the same order as the one in which the tasks where initially added to the job.
        List<Task<?>> results = jppfClient.submitJob(job);

        // process the results
        processExecutionResults(job.getName(), results);
    }

    /**
     * Execute a job in non-blocking mode. The application has the
     * responsibility for handling the notification of job completion and
     * collecting the results.
     *
     * @param jppfClient the {@link JPPFClient} instance which submits the job
     * for execution.
     * @throws Exception if an error occurs while executing the job.
     */
    public void executeNonBlockingJob(final JPPFClient jppfClient) throws Exception {
        // Create a job
        JPPFJob job = createJob("Template non-blocking job");

        // set the job in non-blocking (or asynchronous) mode.
        job.setBlocking(false);

        // Submit the job. This call returns immediately without waiting for the execution of
        // the job to complete. As a consequence, the object returned for a non-blocking job is
        // always null. Note that we are calling the exact same method as in the blocking case.
        jppfClient.submitJob(job);

        // the non-blocking job execution is asynchronous, we can do anything else in the meantime
        System.out.println("Doing something while the job is executing ...");
        // ...

        // We are now ready to get the results of the job execution.
        // We use JPPFJob.awaitResults() for this. This method returns immediately with
        // the results if the job has completed, otherwise it waits until the job execution is complete.
        List<Task<?>> results = job.awaitResults();

        // process the results
        processExecutionResults(job.getName(), results);
    }

    /**
     * Execute multiple jobs in parallel from the same JPPFClient.
     * <p>
     * This is an extension of the {@code executeNonBlockingJob()} method, with
     * one additional step: to ensure that a sufficient number of connections to
     * the server are present, so that jobs can be submitted concurrently. The
     * number of connections determines the number of jobs that can be submitted
     * in parallel. It can be set in the JPPF configuration or dynamically with
     * the {@link JPPFConnectionPool} API.
     * <p>
     * As a result, the call to {@code executeNonBlockingJob(jppfClient)} is
     * effectively equivalent to
     * {@code executeMultipleConccurentJobs(jppfClient, 1)}.
     * <p>
     * There are many patterns that can be applied to parallel job execution,
     * you are encouraged to read the
     * <a href="http://www.jppf.org/doc/v4/index.php?title=Submitting_multiple_jobs_concurrently">dedicated
     * section</a>
     * of the JPPF documentation for details and code samples.
     *
     * @param jppfClient the JPPF client which submits the jobs.
     * @param numberOfJobs the number of jobs to execute.
     * @throws Exception if any error occurs.
     */
    public void executeMultipleConcurrentJobs(final JPPFClient jppfClient, final int numberOfJobs) throws Exception {
        // ensure that the client connection pool has as many connections
        // as the number of jobs to execute
        ensureNumberOfConnections(jppfClient, numberOfJobs);

        // this list will hold all the jobs submitted for execution,
        // so we can later collect and process their results
        final List<JPPFJob> jobList = new ArrayList<>(numberOfJobs);

        // create and submit all the jobs
        for (int i = 1; i <= numberOfJobs; i++) {
            // create a job with a distinct name
            JPPFJob job = createJob("Template concurrent job " + i);

            // set the job in non-blocking (or asynchronous) mode.
            job.setBlocking(false);

            // submit the job for execution, without blocking the current thread
            jppfClient.submitJob(job);

            // add this job to the list
            jobList.add(job);
        }

        // the non-blocking jobs are submitted asynchronously, we can do anything else in the meantime
        System.out.println("Doing something while the jobs are executing ...");
        // ...

        // wait until the jobs are finished and process their results.
        for (JPPFJob job : jobList) {
            // wait if necessary for the job to complete and collect its results
            List<Task<?>> results = job.awaitResults();

            // process the job results
            processExecutionResults(job.getName(), results);
        }
    }

    /**
     * Ensure that the JPPF client has the desired number of connections.
     *
     * @param jppfClient the JPPF client which submits the jobs.
     * @param numberOfConnections the desired number of connections.
     * @throws Exception if any error occurs.
     */
    public void ensureNumberOfConnections(final JPPFClient jppfClient, final int numberOfConnections) throws Exception {
        // wait until the client has at least one connection pool with at least one avaialable connection
        JPPFConnectionPool pool = jppfClient.awaitActiveConnectionPool();

        // if the pool doesn't have the expected number of connections, change its size
        if (pool.getConnections().size() != numberOfConnections) {
            // set the pool size to the desired number of connections
            pool.setSize(numberOfConnections);
        }

        // wait until all desired connections are available (ACTIVE status)
        pool.awaitActiveConnections(Operator.AT_LEAST, numberOfConnections);
    }

    /**
     * Process the execution results of each submitted task.
     *
     * @param jobName the name of the job whose results are processed.
     * @param results the tasks results after execution on the grid.
     */
    public synchronized void processExecutionResults(final String jobName, final List<Task<?>> results) {
        System.out.printf("Results for job '%s' :\n", jobName);
        Integer resultado = 0;
        for (Task<?> task : results) {
            if (task.getException() != null) {
                Exception e = task.getException();
                e.printStackTrace();
            } else {
                String taskName = task.getId();
                System.out.println(taskName + ", execution result: " + task.getResult());
                resultado += (Integer)task.getResult();
            }
        }
        System.out.println("Resultado: " + resultado);
    }
}
