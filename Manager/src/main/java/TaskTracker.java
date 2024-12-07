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

    public boolean addResult(String bucketName, String result) {
        results.put(bucketName, result);
        completedTasks.incrementAndGet();
        return true;
    }

    public int changeTotalTasks(int newTotalTasks) {
        int backoff = 10; // Start with 10ms
        while (!totalTasks.compareAndSet(totalTasks.get(), newTotalTasks)) {
            try {
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, 1000); // Cap at 1 second
            } catch (InterruptedException e) {
                AWS.errorMsg("Manager: Error setting the total task number to %d. Interrupted: %s", newTotalTasks,
                        e.toString());
                Thread.currentThread().interrupt();
                break;
            }
        }
        return totalTasks.get();
    }

    public boolean isAllCompleted() {
        AWS.debugMsg("completedTasks: %d, totalTasks: %d", completedTasks.get(), totalTasks.get());
        return completedTasks.get() == totalTasks.get();
    }

    public ConcurrentHashMap<String, String> getResults() {
        return results;
    }
}