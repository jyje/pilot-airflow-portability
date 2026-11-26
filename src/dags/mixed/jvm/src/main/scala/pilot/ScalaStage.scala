package pilot

import org.apache.airflow.sdk.{Client, Context, Task}

class ScalaStage extends Task {
  override def execute(context: Context, client: Client): Unit = {
    val value = client.getXCom("kotlin").asInstanceOf[String]
    require(value != null, "The Kotlin stage did not return a value")
    client.setXCom(s"$value-scala")
  }
}
