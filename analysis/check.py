import pandas as pd
m = pd.read_csv("results/comparative/messages.csv")
r = pd.read_csv("results/comparative/runs.csv")
a = m[m.scenario.str.startswith("adaptive")].drop(columns="condition").merge(
    r[["run_id", "condition"]], on="run_id")
a = a[a.condition != "dynamic"]
a[["rtt_est", "loss_est"]] = a[["rtt_est", "loss_est"]].apply(pd.to_numeric, errors="coerce")
print(a.groupby(["scenario", "condition"])[["rtt_est", "loss_est"]].median().round(1))