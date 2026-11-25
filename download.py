"""Download USAspending Award Data Archive files (all agencies) and convert each to Parquet.

Run on the Dataproc master node:
    nohup spark-submit --deploy-mode client --py-files to_parquet.py download.py > run.log 2>&1 &
(client mode: the cluster defaults to cluster mode, which runs this script on a worker without
the home disk, hdfs CLI and internet access it needs.)
Per fiscal year: the zip goes to local disk (SCRATCH, ~2 GB), its CSVs are unzipped straight into
HDFS (RAW, ~20 GB), converted, then both are deleted. Only one fiscal year is on disk at a time.
Output: $OUT/contracts and $OUT/assistance (HDFS, default /user/<you>/fedspend/parquet),
partitioned by fy=<source file> and year=<calendar year>.
Safe to re-run: fiscal years whose output already finished are skipped.
"""
import fcntl
import json
import os
import shutil
import subprocess
import urllib.request
import zipfile

from pyspark.sql import SparkSession

from to_parquet import convert

API = "https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/"
# Federal fiscal years run Oct-Sep, so calendar 2022-2025 needs FY2022 through FY2026.
FISCAL_YEARS = range(2022, 2027)
TYPES = ["contracts", "assistance"]
# Relative paths resolve to /user/<you>/ in HDFS.
OUT = os.environ.get("OUT", "fedspend/parquet")
RAW = os.environ.get("RAW", "fedspend/raw")
SCRATCH = os.environ.get("SCRATCH", os.path.expanduser("~/fedspend_scratch"))


def list_files(fiscal_year, award_type):
    body = json.dumps({"agency": "all", "fiscal_year": fiscal_year, "type": award_type}).encode()
    req = urllib.request.Request(API, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        files = json.load(resp)["monthly_files"]
    # "Delta" files only hold recent changes; we want the complete yearly file.
    full = [f for f in files if "Full" in f["file_name"]]
    assert len(full) == 1, f"Expected one full file for FY{fiscal_year} {award_type}: {files}"
    return full[0]


def hdfs(*args, stdin=None):
    subprocess.run(["hdfs", "dfs", *args], stdin=stdin, check=True)


spark = SparkSession.builder.appName("usaspending-download").getOrCreate()


def finished(path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path + "/_SUCCESS")
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


os.makedirs(SCRATCH, exist_ok=True)
# Two runs at once would fight over the same .part files and HDFS folders; the lock fails fast instead.
lock = open(os.path.join(SCRATCH, ".lock"), "w")
try:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit("another download.py is already running; check with: ps aux | grep download.py")
for award_type in TYPES:
    for fy in FISCAL_YEARS:
        # One output folder per fiscal-year file, written in overwrite mode, so a re-run replaces instead of duplicating.
        out_dir = f"{OUT}/{award_type}/fy={fy}"
        if finished(out_dir):
            print("have", out_dir, flush=True)
            continue
        f = list_files(fy, award_type)
        zip_path = os.path.join(SCRATCH, f["file_name"])
        raw_dir = f"{RAW}/{award_type}_FY{fy}"
        if not os.path.exists(zip_path):
            print("downloading", f["file_name"], flush=True)
            # Write to .part first so an interrupted download is never mistaken for a finished one.
            with urllib.request.urlopen(f["url"]) as resp, open(zip_path + ".part", "wb") as out:
                shutil.copyfileobj(resp, out)
            os.rename(zip_path + ".part", zip_path)
        hdfs("-mkdir", "-p", raw_dir)
        with zipfile.ZipFile(zip_path) as z:
            for name in z.namelist():
                if not name.endswith(".csv"):
                    continue
                print("unzipping to HDFS", name, flush=True)
                # Stream each CSV from the zip into HDFS so the unzipped data never touches local disk.
                p = subprocess.Popen(["hdfs", "dfs", "-put", "-f", "-", f"{raw_dir}/{os.path.basename(name)}"],
                                     stdin=subprocess.PIPE)
                with z.open(name) as src:
                    shutil.copyfileobj(src, p.stdin)
                p.stdin.close()
                assert p.wait() == 0, f"hdfs put failed for {name}"
        convert(spark, award_type, f"{raw_dir}/*.csv", out_dir)
        hdfs("-rm", "-r", "-skipTrash", raw_dir)  # skipTrash so the space counts as freed right away
        os.remove(zip_path)

for award_type in TYPES:
    print(award_type, "rows:", spark.read.parquet(f"{OUT}/{award_type}").count())
spark.stop()
print("done")
