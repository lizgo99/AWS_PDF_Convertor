package Manager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskTracker {
    private final String inputFileUrl;
    private final AtomicInteger completedTasks;
    private final AtomicInteger totalTasks;
    private final ConcurrentHashMap<String, String> results;

    public TaskTracker(String inputFileUrl, int totalTasks) {
        this.inputFileUrl = inputFileUrl;
        this.completedTasks = new AtomicInteger(0);
        this.totalTasks = new AtomicInteger(totalTasks);
        this.results = new ConcurrentHashMap<>();
    }

    public String getInputFileUrl() {
        return inputFileUrl;
    }

    public boolean addResult(String pdfUrl, String result) {
        results.put(pdfUrl, result);
        completedTasks.incrementAndGet();
        return true;
    }

    public boolean isAllCompleted() {
        return completedTasks.get() == totalTasks.get();
    }

    public ConcurrentHashMap<String, String> getResults() {
        return results;
    }



}
