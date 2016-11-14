/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ucr.jppf.framework.lib;

import org.jppf.node.event.TaskExecutionEvent;
import org.jppf.node.event.TaskExecutionListener;

/**
 *
 * @author David
 */
public class TaskListener implements TaskExecutionListener {

    @Override
    public synchronized void taskExecuted(TaskExecutionEvent event) {
        System.out.println("Task " + event.getTask().getId() + " completed with result : "
                + event.getTask().getResult());
        System.out.println("cpu time = " + event.getTaskInformation().getCpuTime()
                + ", elapsed = " + event.getTaskInformation().getElapsedTime());
    }

    @Override
    public synchronized void taskNotification(TaskExecutionEvent event) {
        System.out.println("Task " + event.getTask().getId() + " sent user object : "
                + event.getUserObject());
    }
}
