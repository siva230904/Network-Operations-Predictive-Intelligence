Great — **A = High-activity risk**.

For ML1, we can now frame the problem around predicting whether the **next time window (t+1)** will be high-activity, using only information available through time **t**.

### ML1 problem definition

* **Primary problem:** Predict high-activity risk.
* **Prediction unit:** `grid_id + hourly time window`.
* **Prediction horizon:** the next hour, `t+1`.
* **Features:** trailing historical features calculated using data up to and including `t`.
* **Target:** whether activity in hour `t+1` meets the predefined high-activity threshold.
* **Business action:** *Investigate grids flagged as high-activity risk before taking further operational action.*
* **Non-goals:** This model does **not** claim congestion, capacity shortage, throughput limitation, or network fault.

### Critical time boundary

```text
Historical hours ───────────────► prediction point
        t-23 ... t-2  t-1   t
                              │
                              │ features calculated here
                              ▼
                         predict t+1
                              │
                              ▼
                    high-activity label
```

This prevents leakage: the model cannot use `t+1` activity to predict whether `t+1` is high activity.

### One decision still needs to be documented

We need an **exact high-activity threshold** for the target. For example:

```text
target = 1 if total_activity(t+1) >= THRESHOLD
target = 0 otherwise
```

The threshold should be defined explicitly and applied consistently to the training/evaluation data.

If you already have a threshold specified in your Phase 3/NP3 work, we should **reuse that documented threshold rather than invent a new one**.
