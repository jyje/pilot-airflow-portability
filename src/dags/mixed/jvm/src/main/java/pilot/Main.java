package pilot;

import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.Server;
import org.apache.airflow.sdk.Task;

public class Main {
    public static void main(String[] args) {
        String dag = "pilot_mixed_language_chain";
        Server.create(args).serve(new Bundle()
            .register(dag, "java", JavaStage.class)
            .register(dag, "kotlin", KotlinStage.class)
            .register(dag, "scala", ScalaStage.class));
    }

    public static class JavaStage implements Task {
        @Override
        public void execute(Context context, Client client) {
            String value = (String) client.getXCom("go");
            client.setXCom(java.util.Objects.requireNonNull(value) + "-java");
        }
    }
}
