use std::{collections::HashMap, fs::File};

use geohashrust::{BinaryHash, GeoLocation};
use osmtiler2::Writer;

fn main() {
    let osm_pbf_file_path = "data/germany-latest.osm.pbf";
    let binary_hash_precision: u8 = 11;

    let file = File::open(osm_pbf_file_path).unwrap();
    let mut pbf = osmpbfreader::OsmPbfReader::new(file);

    let mut binary_hash_count = HashMap::new();

    pbf.par_iter().for_each(|obj| {
        let obj = obj.unwrap();

        if let osmpbfreader::OsmObj::Node(node) = obj {
            let geo_location = GeoLocation {
                latitude: node.lat(),
                longitude: node.lon(),
            };
            let binary_hash = BinaryHash::encode(&geo_location, binary_hash_precision).to_string();
            *binary_hash_count.entry(binary_hash).or_insert(0) += 1;
        }
    });

    Writer::new(binary_hash_count, binary_hash_precision).unwrap();
}
