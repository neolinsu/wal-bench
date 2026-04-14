import matplotlib.pyplot as plt

concurrency = [1, 2, 3, 4, 5]

p50   = [852, 1536, 2243, 3283, 4259]
p90   = [2315, 3441, 4719, 6039, 6839]
p99   = [2533, 4779, 6039, 7963, 9279]
p999  = [4991, 6627, 6635, 11143, 11263]

# Normalize each percentile by its value at c=1
p50_norm  = [v / p50[0]  for v in p50]
p90_norm  = [v / p90[0]  for v in p90]
p99_norm  = [v / p99[0]  for v in p99]
p999_norm = [v / p999[0] for v in p999]

fig, ax = plt.subplots(figsize=(8, 5))

ax.plot(concurrency, p50_norm,  "o-", label="p50",   linewidth=2)
ax.plot(concurrency, p90_norm,  "s-", label="p90",   linewidth=2)
ax.plot(concurrency, p99_norm,  "^-", label="p99",   linewidth=2)
ax.plot(concurrency, p999_norm, "D-", label="p99.9", linewidth=2)
ax.plot(concurrency, concurrency, "k--", label="y = x (linear)", linewidth=1, alpha=0.5)

ax.set_xlabel("Concurrency")
ax.set_ylabel("Normalized latency (× value at c=1)")
ax.set_title("fdatasync latency scaling (normalized, local SSD, std mode, 512B O_DIRECT)")
ax.set_xticks(concurrency)
ax.legend()
ax.grid(True, alpha=0.3)

fig.tight_layout()
fig.savefig("/data/neolin/dev/nfs-west-east/docs/experiments/local_latency.png", dpi=150)
print("Saved to docs/experiments/local_latency.png")
