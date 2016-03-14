package to.then.aws.swf.kinesis;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.model.ActivityTask;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.PollForActivityTaskRequest;
import com.amazonaws.services.simpleworkflow.model.PollForDecisionTaskRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bridge {

    private static final Logger log = LoggerFactory.getLogger(Bridge.class);
    private final AmazonSimpleWorkflow swf;
    private final AmazonKinesis kinesis;
    private final String domain;
    private final List<TaskList> activityTaskLists;
    private final List<TaskList> decisionTaskLists;
    private final String streamName;
    private final ObjectMapper json;
    private final ExecutorService executor;

    public static void main(String[] args) throws InterruptedException {
        String domain = System.getenv("DOMAIN");
        String activityTaskListNames = System.getenv("ACTIVITY_TASK_LISTS");
        String decisionTaskListNames = System.getenv("DECISION_TASK_LISTS");
        String streamName = System.getenv("STREAM_NAME");
        Region region = (Regions.getCurrentRegion() == null ? Region.getRegion(Regions.US_EAST_1) : Regions.getCurrentRegion());
        Bridge bridge = new Bridge(domain, toTaskLists(activityTaskListNames), toTaskLists(decisionTaskListNames), streamName, new AmazonSimpleWorkflowClient().withRegion(region), new AmazonKinesisClient().withRegion(region));
        bridge.start();
    }

    private static List<TaskList> toTaskLists(String names) {
        List<TaskList> taskLists = new LinkedList();
        if (names != null) {
            for (String name : names.split(",")) {
                String trimmedName = name.trim();
                if (trimmedName.length() > 0) {
                    taskLists.add(new TaskList().withName(trimmedName));
                }
            }
        }
        return taskLists;
    }

    public Bridge(String domain, List<TaskList> activityTaskLists, List<TaskList> decisionTaskLists, String streamName, AmazonSimpleWorkflow swf, AmazonKinesis kinesis) {
        json = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        json.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        json.addMixIn(AmazonWebServiceRequest.class, AmazonWebServiceRequestMixIn.class);
        this.swf = swf;
        this.kinesis = kinesis;
        this.domain = domain;
        this.activityTaskLists = activityTaskLists;
        this.decisionTaskLists = decisionTaskLists;
        this.streamName = streamName;
        int poolSize = Math.max(activityTaskLists.size() + decisionTaskLists.size(), 1);
        log.info("Executor pool size: " + poolSize);
        executor = Executors.newFixedThreadPool(poolSize);
    }

    public void start() {
        Map<TaskList, Runnable> pollTasks = new LinkedHashMap();
        activityTaskLists.forEach((taskList) -> {
            pollTasks.put(taskList, () -> {
                pollForActivityTask(taskList);
            });
        });
        decisionTaskLists.forEach((taskList) -> {
            pollTasks.put(taskList, () -> {
                pollForDecisionTask(taskList);
            });
        });
        pollTasks.forEach((taskList, task) -> {
            executor.execute(() -> {
                while (true) {
                    try {
                        task.run();
                    } catch (Throwable t) {
                        log.error(taskList.getName(), t);
                    }
                }
            });
        });
    }

    public void pollForDecisionTask(TaskList taskList) {
        PollForDecisionTaskRequest poll = new PollForDecisionTaskRequest()
                .withMaximumPageSize(1)
                .withReverseOrder(Boolean.TRUE)
                .withDomain(domain)
                .withTaskList(taskList);
        DecisionTask task = swf.pollForDecisionTask(poll);
        if (task != null && task.getEvents() != null) {
            putRecord(task.getWorkflowExecution().getWorkflowId(), "pollForDecisionTask", poll, "decisionTask", task);
        }
    }

    public void pollForActivityTask(TaskList taskList) {
        PollForActivityTaskRequest poll = new PollForActivityTaskRequest()
                .withDomain(domain)
                .withTaskList(taskList);
        ActivityTask task = swf.pollForActivityTask(poll);
        if (task != null && task.getActivityType() != null) {
            putRecord(task.getWorkflowExecution().getWorkflowId(), "pollForActivityTask", poll, "activityTask", task);
        }
    }

    private void putRecord(String workflowId, String requestType, Object request, String taskType, Object task) {
        try {
            Map<String, Object> record = new LinkedHashMap();
            record.put(requestType, request);
            record.put(taskType, task);
            byte[] recordJson = json.writeValueAsBytes(record);
            PutRecordResult result = kinesis.putRecord(streamName, ByteBuffer.wrap(recordJson), workflowId);
            log.info(streamName + " -> " + workflowId + ": " + new String(recordJson));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

}
