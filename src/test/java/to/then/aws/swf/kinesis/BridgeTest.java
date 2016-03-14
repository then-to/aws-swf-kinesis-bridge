package to.then.aws.swf.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.model.DecisionTask;
import com.amazonaws.services.simpleworkflow.model.PollForDecisionTaskRequest;
import com.amazonaws.services.simpleworkflow.model.TaskList;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeTest {

    private static Logger log = LoggerFactory.getLogger(BridgeTest.class);
    private Bridge bridge;
    private AmazonSimpleWorkflow swf;
    private AmazonKinesis kinesis;
    private String domain;
    private String taskList;
    private String streamName;
    private ObjectMapper json;

    @Before
    public void before() {
        domain = "swf-domain";
        taskList = "swf-tasklist";
        streamName = "kinesis-streamname";
        swf = mock(AmazonSimpleWorkflow.class);
        kinesis = mock(AmazonKinesis.class);
        bridge = new Bridge(domain, Collections.EMPTY_LIST, Collections.singletonList(new TaskList().withName(taskList)), streamName, swf, kinesis);
        json = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void startWorkflow() throws InterruptedException, JsonProcessingException, IOException {
        when(swf.pollForDecisionTask(any(PollForDecisionTaskRequest.class)))
                .thenReturn(json.readValue(BridgeTest.class.getResourceAsStream("task.json"),
                        DecisionTask.class));
        bridge.pollForDecisionTask(new TaskList().withName(taskList));
        ArgumentCaptor<ByteBuffer> argumentCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(kinesis).putRecord(eq(streamName), argumentCaptor.capture(), eq("ExampleWorkflow-1"));
        byte[] data = argumentCaptor.getValue().array();
        assertEquals(595, data.length);
        JsonNode result = json.readTree(data);
        PollForDecisionTaskRequest poll = json.convertValue(result.get("pollForDecisionTask"), PollForDecisionTaskRequest.class);
        assertEquals("swf-domain", poll.getDomain());
        assertEquals("swf-tasklist", poll.getTaskList().getName());
        DecisionTask task = json.convertValue(result.get("decisionTask"), DecisionTask.class);
        assertEquals("ExampleWorkflow", task.getWorkflowType().getName());
        assertEquals("ZXJhdCwgdXQgcGhhcmV0cmEgbGVjdHV==", task.getTaskToken());
        assertNotNull(task.getEvents());
        assertEquals(1, task.getEvents().size());
    }

}
