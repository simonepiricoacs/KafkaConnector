package it.water.connectors.kafka.model;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.water.core.validation.annotations.NoMalitiusCode;
@JsonIgnoreProperties
public class ConnectorTask {
    @NoMalitiusCode
    private String connector;
    private int task;
    public String getConnector() {
        return connector;
    }
    public void setConnector(String connector) {
        this.connector = connector;
    }
    public int getTask() {
        return task;
    }
}
