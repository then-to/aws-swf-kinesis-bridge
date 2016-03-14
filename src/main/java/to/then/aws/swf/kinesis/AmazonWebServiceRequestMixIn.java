package to.then.aws.swf.kinesis;

import com.amazonaws.RequestClientOptions;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.metrics.RequestMetricCollector;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;


public abstract class AmazonWebServiceRequestMixIn {
 
    @JsonIgnore
    public abstract RequestMetricCollector getRequestMetricCollector();
    
    @JsonIgnore
    public abstract AWSCredentials getRequestCredentials();
    
    @JsonIgnore
    public abstract RequestClientOptions getRequestClientOptions();
    
    @JsonIgnore
    public abstract ProgressListener getGeneralProgressListener();
    
    @JsonIgnore
    public abstract Map<String, String> getCustomRequestHeaders();
    
    @JsonIgnore
    public abstract int getReadLimit();
    
}
