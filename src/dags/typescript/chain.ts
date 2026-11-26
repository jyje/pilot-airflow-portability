import { Bundle, TaskHandler } from "@apache-airflow/ts-sdk";
import { appendJavaScript } from "./javascript.js";

function appendTypeScript({ value }: { value: string }): string {
  return `${value}-typescript`;
}

const dagId = "pilot_mixed_language_chain";
const bundle = new Bundle();
bundle.register(new TaskHandler(dagId, "typescript", appendTypeScript));
bundle.register(new TaskHandler(dagId, "javascript", appendJavaScript));
await bundle.serve();
