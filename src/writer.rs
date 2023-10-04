use std::collections::HashMap;

use geohashrust::BinaryHash;
use polars::prelude::*;

pub struct Writer {}

impl Writer {
    pub fn new(
        binary_hash_count: HashMap<String, i32>,
        binary_hash_precision: u8,
    ) -> Result<(), PolarsError> {
        let max_allowed_features_in_binary_hash = 2000;

        let node_count: Vec<i32> = binary_hash_count.clone().into_values().collect();
        let binary_hash: Vec<String> = binary_hash_count.clone().into_keys().collect();

        let mut binary_hash_count_df = df!(
            "node_count" => node_count,
            "binary_hash" => binary_hash
        )?;

        let mut binary_hash_results = HashMap::new();

        for i in 0..binary_hash_precision as usize {
            let sliced_binary_hash: Vec<&str> = binary_hash_count_df
                .column("binary_hash")?
                .utf8()?
                .into_no_null_iter()
                .map(|binary_hash_value: &str| &binary_hash_value[..i + 1])
                .collect();
            let temp_binary_hash_count_df = binary_hash_count_df
                .with_column(Series::new("sliced_binary_hash", sliced_binary_hash))?
                .clone();

            let grouped_binary_hash_df = temp_binary_hash_count_df
                .lazy()
                .group_by([col("sliced_binary_hash")])
                .agg([
                    col("node_count").sum().alias("total_node_count"),
                    col("binary_hash").reverse().alias("binary_hashes"),
                ])
                .collect()?;
            let binary_hashes_over_max_allowed_features_df = grouped_binary_hash_df
                .clone()
                .lazy()
                .filter(col("total_node_count").gt(lit(max_allowed_features_in_binary_hash)))
                .collect()?
                .explode(["binary_hashes"])?
                .rename("binary_hashes", "binary_hash")?
                .drop_many(&["sliced_binary_hash", "total_node_count"])
                .left_join(&binary_hash_count_df, ["binary_hash"], ["binary_hash"])?;
            binary_hash_count_df = binary_hashes_over_max_allowed_features_df;

            let binary_hashes_under_max_allowed_features_df = grouped_binary_hash_df
                .lazy()
                .filter(col("total_node_count").lt(lit(max_allowed_features_in_binary_hash + 1)))
                .collect()?;
            let sliced_binary_hash_list: Vec<String> = binary_hashes_under_max_allowed_features_df
                .column("sliced_binary_hash")?
                .utf8()?
                .into_no_null_iter()
                .map(|geohash| geohash.to_string())
                .collect();
            let node_count_list: Vec<i32> = binary_hashes_under_max_allowed_features_df
                .column("total_node_count")?
                .i32()?
                .into_no_null_iter()
                .collect();

            for (node_count, sliced_binary_hash) in node_count_list
                .into_iter()
                .zip(sliced_binary_hash_list.into_iter())
            {
                binary_hash_results.insert(sliced_binary_hash, node_count);
            }
        }

        let binary_hash_list: Vec<String> = binary_hash_count_df
            .column("binary_hash")?
            .utf8()?
            .into_no_null_iter()
            .map(|geohash| geohash.to_string())
            .collect();
        let node_count_list: Vec<i32> = binary_hash_count_df
            .column("node_count")?
            .i32()?
            .into_no_null_iter()
            .collect();

        for (node_count, binary_hash) in node_count_list
            .into_iter()
            .zip(binary_hash_list.into_iter())
        {
            binary_hash_results.insert(binary_hash, node_count);
        }

        let output_file_path = std::path::Path::new("binary_hashes.csv");
        let mut output_writer = csv::Writer::from_path(output_file_path).unwrap();
        output_writer
            .serialize(vec![
                "binary_hash",
                "node_count",
                "min_lon",
                "min_lat",
                "max_lon",
                "max_lat",
            ])
            .expect("CSV: unable to write binary hash header");
        for (binary_hash, node_count) in binary_hash_results.into_iter() {
            let bh = BinaryHash::from_string(binary_hash.as_str());
            let bbox = bh.decode();
            output_writer
                .serialize((
                    binary_hash,
                    node_count,
                    bbox.min_lon,
                    bbox.min_lat,
                    bbox.max_lon,
                    bbox.max_lat,
                ))
                .expect("CSV: unable to write geohash");
        }

        Ok(())
    }
}
