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
# df = pd.read_csv("tps (hourly).csv")

# # If hour_ts is a string, parse it as datetime
# df["hour_ts"] = pd.to_datetime(df["hour_ts"])

# # Sum avg_tps across shards for each (epoch, hour)
# df_hourly = (
#     df.groupby(["epoch_index", "hour_ts"])["avg_tps"]
#       .sum()
#       .reset_index()
#       .rename(columns={"avg_tps": "hourly_avg_tps"})
# )

# # Save result
# df_hourly.to_csv("epoch_hourly_tps.csv", index=False)

# print(df_hourly)








# import pandas as pd

# # Load your CSV
# df = pd.read_csv("tps (hourly).csv")  # replace with your CSV file name

# # Ensure tx_shard is numeric
# df["tx_shard"] = pd.to_numeric(df["tx_shard"], errors="coerce")

# # Filter only shard 6
# shard6_df = df[df["tx_shard"] == 7][["epoch_index", "avg_tps"]]

# # Save to a new CSV
# shard6_df.to_csv("shard7_epoch_tps.csv", index=False)

# print(shard6_df)




# import pandas as pd

# # Load CSV
# df = pd.read_csv("tps (hourly).csv")

# # Ensure shard column is numeric
# df["tx_shard"] = pd.to_numeric(df["tx_shard"], errors="coerce")

# # Parse hour timestamp (change column name if needed)
# df["hour_ts"] = pd.to_datetime(df["hour_ts"])

# # Filter shard 7
# shard7_df = df[df["tx_shard"] == 0]

# # Hourly average TPS
# hourly_tps = (
#     shard7_df
#     .groupby("hour_ts", as_index=False)["avg_tps"]
#     .mean()
# )

# # Save
# hourly_tps.to_csv("shard0_hourly_tps.csv", index=False)

# print(hourly_tps)







import pandas as pd

# Load your CSV
df = pd.read_csv("txLatencyHourly.csv")

# If hour_ts is a string, parse it as datetime
df["tx_hour"] = pd.to_datetime(df["tx_hour"])

# Sum avg_tps across shards for each (epoch, hour)
df_hourly = (
    df.groupby(["epoch_index", "tx_hour"])["avg_latency_sec"]
      .mean()
      .reset_index()
      .rename(columns={"avg_latency_sec": "hourly_avg_latency_sec"})
)

# Save result
df_hourly.to_csv("epoch_hourly_latency.csv", index=False)
print(df_hourly)









# import pandas as pd

# # Load your CSV
# df = pd.read_csv("txLatencyHourly.csv")  # replace with your CSV file name

# # Ensure tx_shard is numeric
# df["rx_shard"] = pd.to_numeric(df["rx_shard"], errors="coerce")

# # Filter only shard 7
# shard7_df = df[df["rx_shard"] == 7][["epoch_index", "avg_latency_sec"]]

# # Save to a new CSV
# shard7_df.to_csv("shard7_epoch_latency.csv", index=False)

# print(shard7_df)