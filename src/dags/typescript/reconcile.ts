import { Bundle, Dag } from "@apache-airflow/ts-sdk";

interface Settlement {
  sales: number;
  refunds: number;
  net: number;
  valid: boolean;
}

const dag = new Dag("pilot_ts_settlement", {
  queue: "typescript",
  isPausedUponCreation: false,
  tags: ["pilot", "typescript"],
});
const collect = dag.taskGroup("collect");
const sales = collect.task("sales", async () => [120, 80, 40])();
const refunds = collect.task("refunds", async () => [15, 5])();

const settlement = dag.task(
  "reconcile",
  async ({ sales, refunds }: { sales: number[]; refunds: number[] }): Promise<Settlement> => {
    const gross = sales.reduce((sum, amount) => sum + amount, 0);
    const returned = refunds.reduce((sum, amount) => sum + amount, 0);
    return { sales: gross, refunds: returned, net: gross - returned, valid: returned <= gross };
  },
)({ sales, refunds });

const published = dag.task("publish", async ({ report }: { report: Settlement }) => report.net)(
  { report: settlement },
);
const quarantined = dag.task(
  "quarantine",
  async ({ report }: { report: Settlement }) => ({ reason: "Refunds exceed sales", net: report.net }),
)({ report: settlement });

dag.if(async function can_publish({ report }: { report: Settlement }) {
  return report.valid;
}, { report: settlement }).then(published).else(quarantined);

dag.task(
  "audit",
  async ({ report }: { report: Settlement }) => report,
  { triggerRule: "none_failed_min_one_success" },
)({ report: settlement }).after(published, quarantined);

await new Bundle(dag).serve();
