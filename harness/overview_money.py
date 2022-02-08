from os import listdir
from os.path import isdir, isfile, join
from txperf.checks.result import Result
import re
import json
import argparse
import jinja2
from sh import gnuplot, rm

OVERVIEW = """
set terminal png size 1600,1200
set output "overview.png"

set multiplot

set key right bottom
set lmargin 10
set rmargin 10
set xrange [0:{{ threads_boundary }}]

set size 1, 0.5
set origin 0, 0

set tmargin 0
set bmargin 3
set xlabel "total threads (num. clients x thread per client)"
set yrange [0:{{ p50_boundary }}]
set y2range [0:{{ p50_boundary }}]
unset ytics
set y2tics auto
set ylabel "p50 latency (ms)"

plot "latency.data" using 1:($2/1000) notitle with line lt rgb "black"

set size 1, 0.5
set origin 0, 0.5

set title "{{ title }}"
show title

set tmargin 3
set bmargin 0
unset xlabel
unset y2tics
set ytics
set format x ""
set yrange [0:{{ tps_boundary }}]
set ylabel "avg throughput (tps)"

plot "throughput.data" using 1:2 notitle with line lt rgb "black"

unset multiplot
"""

result_pattern = re.compile("""^\d+\.json$""")

def review(dir_path):
    suite_path = None
    for f in listdir(dir_path):
        if not isfile(join(dir_path,f)):
            continue
        if f != "all.json" and not result_pattern.match(f):
            continue
        suite_path = join(dir_path,f)
    
    result = None
    with open(suite_path, "r") as result_file:
        result = json.load(result_file)
    
    best = dict()
    for test_key in result["test_runs"].keys():
        for test_run_key in result["test_runs"][test_key].keys():
            info = None
            if not isfile(join(dir_path, test_run_key, "info.json")):
                continue
            with open(join(dir_path, test_run_key, "info.json"), "r") as info_file:
                info = json.load(info_file)
            if info["result"] != Result.PASSED:
                continue
            nodes = len(info["workload"]["nodes"])
            producers = info["workload"]["settings"]["producers"]
            stat_key = None
            if nodes == 1:
                stat_key = info["workload"]["nodes"][0]
            else:
                stat_key = "total"
            if stat_key not in info["workload"]["stat"]:
                continue
            throughput = info["workload"]["stat"][stat_key]["throughput"]["avg/s"]
            p50 = info["workload"]["stat"][stat_key]["latency_us"]["tx"]["p50"]
            if producers not in best or throughput > best[producers]["throughput"]:
                best[producers] = {
                    "throughput": throughput,
                    "p50": p50,
                    "threads": nodes * producers
                }
    
    data = list(best.values())
    data.sort(key=lambda x:x["threads"])
    return data

parser = argparse.ArgumentParser(description='build money cross chart')
parser.add_argument('--title', required=True)
parser.add_argument('--path', required=True)
args = parser.parse_args()

data = review(args.path)

threads_boundary = 0
p50_boundary = 0
tps_boundary = 0

latency = open(join(args.path, f"latency.data"), "w")
throughput = open(join(args.path, f"throughput.data"), "w")

for item in data:
    tps_boundary = max(tps_boundary, item["throughput"])
    p50_boundary = max(p50_boundary, item["p50"])
    threads_boundary = max(threads_boundary, item["threads"])
    latency.write(f"{item['threads']}\t{item['p50']}\n")
    throughput.write(f"{item['threads']}\t{item['throughput']}\n")

latency.close()
throughput.close()

with open(join(args.path, f"overview.gnuplot"), "w") as gnuplot_file:
    gnuplot_file.write(
        jinja2.Template(OVERVIEW).render(
            threads_boundary=threads_boundary+2,
            p50_boundary=int(1.2 * p50_boundary / 1000),
            title=args.title,
            tps_boundary=int(1.2 * tps_boundary)))

gnuplot(join(args.path, f"overview.gnuplot"), _cwd=args.path)

rm(join(args.path, f"overview.gnuplot"))
rm(join(args.path, f"latency.data"))
rm(join(args.path, f"throughput.data"))