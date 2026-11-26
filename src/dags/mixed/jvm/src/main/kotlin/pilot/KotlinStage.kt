package pilot

import org.apache.airflow.sdk.Client
import org.apache.airflow.sdk.Context
import org.apache.airflow.sdk.Task

class KotlinStage : Task {
    override fun execute(context: Context, client: Client) {
        val value = requireNotNull(client.getXCom(taskId = "java") as String?)
        client.setXCom(value = "$value-kotlin")
    }
}
