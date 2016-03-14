package to.then.aws.swf.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.model.StartWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeIT {

    private static Logger log = LoggerFactory.getLogger(BridgeIT.class);
    private Bridge bridge;
    private AmazonSimpleWorkflow swf;
    private AmazonKinesis kinesis;
    private String domain;
    private TaskList taskList;
    private String streamName;
    private ObjectMapper json;

    @Before
    public void before() {
        domain = "greeter";
        taskList = new TaskList().withName("greetings");
        streamName = "swf-greeter";
        swf = new AmazonSimpleWorkflowClient();
        kinesis = new AmazonKinesisClient();
        bridge = new Bridge(domain, Collections.EMPTY_LIST, Collections.singletonList(taskList), streamName, swf, kinesis);
        bridge.start();
        json = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void startWorkflow() throws InterruptedException, JsonProcessingException, IOException {
        swf.startWorkflowExecution(new StartWorkflowExecutionRequest()
                .withDomain("greeter")
                .withTaskList(taskList)
                .withWorkflowId("greeting-" + System.currentTimeMillis())
                .withWorkflowType(
                        new WorkflowType().withName("Greet").withVersion("1.0")));
    }

}
