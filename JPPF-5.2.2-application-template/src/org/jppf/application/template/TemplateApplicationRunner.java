package org.jppf.application.template;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

import org.jppf.client.*;
import org.jppf.node.protocol.Task;

public class TemplateApplicationRunner {

    public static final int TAREAS = 8;
    public static final String ARCHIVO = "numeros.txt";

    public static void main(final String... args) {

        try (JPPFClient jppfClient = new JPPFClient()) {
            TemplateApplicationRunner runner = new TemplateApplicationRunner();
            runner.executeBlockingJob(jppfClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public JPPFJob createJob(final String jobName) throws Exception {
        File file;
        FileChannel fileChannel;
        long size;
        long from = 0, to = 0;
        Task<?> task;

        JPPFJob job = new JPPFJob();
        job.setName(jobName);

        file = new File(ARCHIVO);
        fileChannel = new RandomAccessFile(file, "r").getChannel();
        size = fileChannel.size();
        fileChannel.close();
        
        for(int i = 0; i < TAREAS; i++){
            to = from + size/TAREAS;
            System.out.println(from + ":" + to);
            task = job.add(new TemplateJPPFTask(ARCHIVO, from, to));
            task.setId("task " + (i + 1));
            from = to;
        }

        return job;
    }
    public void executeBlockingJob(final JPPFClient jppfClient) throws Exception {
        JPPFJob job = createJob("Template blocking job");
        job.setBlocking(true);
        List<Task<?>> results = jppfClient.submitJob(job);
        processExecutionResults(job.getName(), results);
    }
    public void executeNonBlockingJob(final JPPFClient jppfClient) throws Exception {
        JPPFJob job = createJob("Template non-blocking job");
        job.setBlocking(false);
        jppfClient.submitJob(job);
        System.out.println("Doing something while the job is executing ...");
        List<Task<?>> results = job.awaitResults();
        processExecutionResults(job.getName(), results);
    }
    public void executeMultipleConcurrentJobs(final JPPFClient jppfClient, final int numberOfJobs) throws Exception {
        ensureNumberOfConnections(jppfClient, numberOfJobs);
        final List<JPPFJob> jobList = new ArrayList<>(numberOfJobs);
        for (int i = 1; i <= numberOfJobs; i++) {
            JPPFJob job = createJob("Template concurrent job " + i);
            job.setBlocking(false);
            jppfClient.submitJob(job);
            jobList.add(job);
        }
        System.out.println("Doing something while the jobs are executing ...");
        for (JPPFJob job : jobList) {
            List<Task<?>> results = job.awaitResults();
            processExecutionResults(job.getName(), results);
        }
    }
    
    public void ensureNumberOfConnections(final JPPFClient jppfClient, final int numberOfConnections) throws Exception {
        JPPFConnectionPool pool = jppfClient.awaitActiveConnectionPool();
        if (pool.getConnections().size() != numberOfConnections) {
            pool.setSize(numberOfConnections);
        }
        pool.awaitActiveConnections(Operator.AT_LEAST, numberOfConnections);
    }
    
    public synchronized void processExecutionResults(final String jobName, final List<Task<?>> results) {
        System.out.printf("Results for job '%s' :\n", jobName);
        Long resultado = 0L;
        for (Task<?> task : results) {
            if (task.getException() != null) {
                Exception e = task.getException();
                e.printStackTrace();
            } else {
                String taskName = task.getId();
                System.out.println(taskName + ", resultado: " + task.getResult());
                resultado += (Long)task.getResult();
            }
        }
        System.out.println("Resultado: " + resultado);
    }
}