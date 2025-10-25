import json
import gc
import math

import pandas as pd
from pathlib import Path
from tqdm import tqdm


FINAL_PARQUET = "mpd.parquet"

def load(files):
    records = []
    for file in tqdm(files, desc=f"Processing {len(files)} slices"):
        with open(file, "r", encoding="utf-8") as f:
            js = json.load(f)
            for pl in js["playlists"]:
                for tr in pl["tracks"]:
                    records.append({
                        "playlist_id": pl["pid"],
                        "playlist_name": pl.get("name", ""),
                        "modified_at": pl["modified_at"],
                        "track_uri": tr["track_uri"],
                        "track_name": tr["track_name"],
                        "artist_name": tr["artist_name"],
                        "album_name": tr["album_name"],
                        "pos": tr["pos"],
                        "duration_ms": tr["duration_ms"]
                    })
    return pd.DataFrame(records)


def main(count=4):
    data_dir = Path("data")
    files = sorted(data_dir.glob("mpd.slice.*.json"))

    chunk_size = math.ceil(len(files) / count)
    chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]

    parquet_files = [f"mpd.part{i+1}.parquet" for i in range(len(chunks))]

    for i, slice_files in enumerate(chunks):
        parquet_path = parquet_files[i]

        df = load(slice_files)
        df.to_parquet(parquet_path, index=False)

    dfs = [pd.read_parquet(p) for p in parquet_files]
    full_df = pd.concat(dfs, ignore_index=True)

    full_df.to_parquet(FINAL_PARQUET, index=False)

    return full_df.head(10)


if __name__ == "__main__":
    main()