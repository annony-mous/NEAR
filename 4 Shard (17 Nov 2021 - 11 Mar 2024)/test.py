# import pandas as pd

# # Load your CSV
# df = pd.read_csv("tps.csv")  # replace with your CSV file name

# # Sum avg_tps for each epoch_index
# df_sum = df.groupby("epoch_index")["avg_tps"].sum().reset_index()

# # Rename column for clarity
# df_sum.rename(columns={"avg_tps": "total_avg_tps"}, inplace=True)

# # Save to new CSV
# df_sum.to_csv("total_epoch_tps.csv", index=False)
# print(df_sum)










# import pandas as pd

# # Load your CSV
# df = pd.read_csv("tps.csv")  # replace with your CSV file name

# # Ensure tx_shard is numeric
# df["tx_shard"] = pd.to_numeric(df["tx_shard"], errors="coerce")

# # Filter only shard 6
# shard6_df = df[df["tx_shard"] == 0][["epoch_index", "avg_tps"]]

# # Save to a new CSV
# shard6_df.to_csv("shard0_epoch_tps.csv", index=False)

# print(shard6_df)








import pandas as pd

# Load your CSV
df = pd.read_csv("txLatency.csv")  # replace with your CSV file name

# Sum avg_tps for each epoch_index
df_sum = df.groupby("epoch_index")["avg_latency_sec"].mean().reset_index()

# Rename column for clarity
df_sum.rename(columns={"avg_latency_sec": "total_avg_latency_sec"}, inplace=True)

# Save to new CSV
df_sum.to_csv("total_epoch_latency.csv", index=False)
print(df_sum)









# import pandas as pd

# # Load your CSV
# df = pd.read_csv("txLatency.csv")  # replace with your CSV file name

# # Ensure tx_shard is numeric
# df["shard_id"] = pd.to_numeric(df["shard_id"], errors="coerce")

# # Filter only shard 0
# shard0_df = df[df["shard_id"] == 3][["epoch_index", "avg_latency_sec"]]

# # Save to a new CSV
# shard0_df.to_csv("shard3_epoch_latency.csv", index=False)

# print(shard0_df)