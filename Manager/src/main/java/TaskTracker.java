import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class TaskTracker {
    private final String inputFileUrl;
    private final AtomicInteger completedTasks;
    private final AtomicInteger totalTasks;
    private final ConcurrentLinkedQueue<String> resultsList;

    public TaskTracker(String inputFileUrl, int totalTasks) {
        this.inputFileUrl = inputFileUrl;
        this.completedTasks = new AtomicInteger(0);
        this.totalTasks = new AtomicInteger(totalTasks);
        this.resultsList = new ConcurrentLinkedQueue<>();
    }

    public String getInputFileUrl() {
        return inputFileUrl;
    }

    public int changeTotalTasks(int newTotalTasks) {
        int oldVal;
        do {
            oldVal = totalTasks.get();
            if (oldVal == newTotalTasks) {
                // No change needed
                return oldVal;
            }
        } while (!totalTasks.compareAndSet(oldVal, newTotalTasks));
        return newTotalTasks;
    }

    public boolean isAllCompleted() {
        AWS.debugMsg("completedTasks: %d, totalTasks: %d", completedTasks.get(), totalTasks.get());
        return completedTasks.get() == totalTasks.get();
    }

    public void addResultToList(String result) {
        AWS.debugMsg("Adding result to list: %s", result);
        resultsList.add(result);
        completedTasks.incrementAndGet();
        AWS.debugMsg("Results list size: %d", resultsList.size());
    }

    public ConcurrentLinkedQueue<String> getResultsList() {
        return resultsList;
    }
}