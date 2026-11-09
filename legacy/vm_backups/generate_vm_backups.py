"""Legacy Defuse Kit | VM Backups legacy export generator (bad on purpose).

Builds Azure Backup-style exports for Zontique's VMs, in the same world as the CMDB:
every VM lives in a CMDB Azure resource group (rg-<app>-<tier>-<env>-<region>-<nnn>), so
backups link VM -> resource group -> application -> cost center -> budget.

Output (period 2026-09-01 .. 2026-10-31, FY2026):
  jobs/backup_jobs_YYYYMMDD.csv        one file per day (fact: one backup job)
  storage/backup_storage_YYYYMM.csv    one file per month (one protected VM per month)
  answer-keys/vm-backups/answer_key.csv, expected_totals.json

Usage: python generate_vm_backups.py <cmdb assets.csv> <output folder> [--seed 42]
Same seed + same CMDB file = identical files.
"""
import csv, json, os, random, sys, uuid
from datetime import date, datetime, timedelta

args = [a for a in sys.argv[1:] if not a.startswith("--")]
CMDB, OUT = args[0], args[1]
SEED = int(sys.argv[sys.argv.index("--seed") + 1]) if "--seed" in sys.argv else 42
rnd = random.Random(SEED)
START, END = date(2026, 9, 1), date(2026, 10, 31)
DRIFT_FROM = date(2026, 9, 16)                       # D07
MISSING_DAYS = {date(2026, 9, 21), date(2026, 10, 12)}  # D08: one per month
GB_PRICE = 0.05                                      # USD per GB-month (illustrative)

# ---------------------------------------------------------------- VMs from the CMDB
rgs = []
with open(CMDB, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        if r["asset_type"] != "AzureResourceGroup" or r["asset_name"] != r["asset_name"].lower():
            continue  # skip the CMDB's own upper-case duplicates
        p = r["asset_name"].split("-")  # rg app tier env region nnn
        rgs.append(dict(rg=r["asset_name"], app=p[1], tier=p[2], env=p[3], region=p[4],
                        sub=r["cloud_native_id"].split("/")[2],
                        deleted=r["deleted_date"] or None, asset_id=r["asset_id"]))

PER_TIER = {"web": 2, "api": 2, "app": 2, "data": 2, "sec": 1, "mon": 1, "net": 0}
vms, answer = [], []


def add_answer(defect, file, record, vm, detail):
    answer.append(dict(defect_id=defect, file_name=file, record_id=record, vm_name=vm, detail=detail))


for g in rgs:
    n = PER_TIER.get(g["tier"], 1)
    if g["env"] in ("dev", "test"):
        n = min(n, 1)
    if g["deleted"]:
        n = max(n, 1)  # deleted resource groups had VMs: their backups are the orphans
    for i in range(1, n + 1):
        name = f"vm-{g['app']}-{g['tier']}-{g['env']}-{i:02d}"
        protected = g["env"] in ("prod", "shared") or g["deleted"] or rnd.random() < (0.5 if g["env"] == "test" else 0.3)
        vms.append(dict(name=name, rg=g["rg"], env=g["env"], region=g["region"], sub=g["sub"],
                        deleted=date.fromisoformat(g["deleted"]) if g["deleted"] else None,
                        protected=bool(protected), in_cmdb=True, base_gb=rnd.randint(20, 2000)))

# D02: 5 VMs in resource groups the CMDB doesn't know
for app, tier, region in [("sapfi", "app", "westeurope"), ("sapfi", "data", "westeurope"), ("legerp", "app", "northeurope"),
                          ("legerp", "data", "northeurope"), ("ftpgw", "web", "eastus2")]:
    rg = f"rg-{app}-{tier}-prod-{region}-001"
    vms.append(dict(name=f"vm-{app}-{tier}-prod-01", rg=rg, env="prod", region=region,
                    sub="ae340454-cac5-468c-a8f4-9481a0a04dc4", deleted=None, protected=True, in_cmdb=False,
                    base_gb=rnd.randint(200, 2000)))
    add_answer("D02", "", f"vm-{app}-{tier}-prod-01", f"vm-{app}-{tier}-prod-01", f"resource group {rg} not in CMDB")

# D01b: 3 protected VMs in active groups deleted during the period
active_prot = [v for v in vms if v["protected"] and not v["deleted"] and v["in_cmdb"]]
for v, d in zip(rnd.sample(active_prot, 3), [date(2026, 9, 9), date(2026, 9, 24), date(2026, 10, 6)]):
    v["deleted"] = d

# D03: 4 production VMs with no Completed job in the last 7 days
prod_ok = [v for v in vms if v["env"] == "prod" and v["protected"] and not v["deleted"] and v["in_cmdb"]]
failing = set(v["name"] for v in rnd.sample(prod_ok, 4))
for n in sorted(failing):
    add_answer("D03", "", n, n, "no Completed job 2026-10-25..2026-10-31 (unprotected production VM)")

# D04: 15% of VMs written differently in part of the files
variant = {}
for v in rnd.sample(vms, round(len(vms) * 0.15)):
    variant[v["name"]] = v["name"].upper() if rnd.random() < 0.5 else v["name"] + ".zontique.local"
    add_answer("D04", "", v["name"], v["name"], f"name also written as {variant[v['name']]}")


def shown(v):
    return variant[v["name"]] if v["name"] in variant and rnd.random() < 0.5 else v["name"]


def vault(v):
    return f"rsv-{'prod' if v['env'] in ('prod', 'shared') else 'nonprod'}-{v['region']}"


def rid(v):
    return (f"/subscriptions/{v['sub']}/resourceGroups/{v['rg']}/providers/"
            f"Microsoft.Compute/virtualMachines/{v['name']}")


# ---------------------------------------------------------------- jobs (one file per day)
days = [START + timedelta(d) for d in range((END - START).days + 1)]
d09_days = set(rnd.sample([d for d in days if d not in MISSING_DAYS], 2))  # D09: ~2% of files
jobs_by_day = {d: [] for d in days}
for v in vms:
    if not v["protected"]:
        continue
    daily = v["env"] in ("prod", "shared")
    for d in days:
        if v["deleted"] and d >= v["deleted"]:
            break
        if not daily and d.weekday() != 6:  # Weekly-12w on Sundays
            continue
        start = datetime(d.year, d.month, d.day, 22, 0) + timedelta(minutes=rnd.randint(0, 6 * 60))
        start = start - timedelta(days=0)  # jobs run 22:00-04:00; the file date is the start date
        if start.date() != d:
            start = datetime(d.year, d.month, d.day, rnd.randint(0, 3), rnd.randint(0, 59))
        status = rnd.choices(["Completed", "CompletedWithWarnings", "Failed"], [95, 3, 2])[0]
        if v["name"] in failing and d >= END - timedelta(6):
            status = "Failed"
        size = round(v["base_gb"] * rnd.uniform(0.01, 0.08), 2) if status != "Failed" else None
        job = dict(JOB_ID=str(uuid.UUID(int=rnd.getrandbits(128))), VM=v, START=start,
                   END=start + timedelta(minutes=rnd.randint(5, 120)), STATUS=status, SIZE=size,
                   ERROR=rnd.choice(["UserErrorVmNotRunning", "ExtensionSnapshotFailed", "BackUpOperationFailed"])
                   if status == "Failed" else "")
        if status == "Completed" and rnd.random() < 0.005:  # D05
            job["SIZE"] = rnd.choice([0, -round(rnd.uniform(1, 50), 2)])
            add_answer("D05", f"backup_jobs_{d:%Y%m%d}.csv", job["JOB_ID"], v["name"], f"Completed with size {job['SIZE']}")
        jobs_by_day[d].append(job)

os.makedirs(f"{OUT}/jobs", exist_ok=True)
file_rows, raw_rows, unique_jobs = {}, 0, set()
for d in days:
    if d in MISSING_DAYS:
        add_answer("D08", f"backup_jobs_{d:%Y%m%d}.csv", "", "", "daily file missing (completeness gap)")
        continue
    rows = list(jobs_by_day[d])
    prev = d - timedelta(1)
    overlap = [j for j in jobs_by_day.get(prev, []) if j["START"].hour >= 23]  # D06
    rows += overlap
    drift = d >= DRIFT_FROM
    ts = (lambda t: t.strftime("%d.%m.%Y %H:%M")) if d in d09_days else (lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ"))
    size_col = "BackupSizeGB" if drift else "BACKUP_SIZE_GB"
    cols = ["JOB_ID", "VM_NAME", "VM_RESOURCE_ID", "VAULT_NAME", "POLICY_NAME", "JOB_START_UTC", "JOB_END_UTC",
            "STATUS", size_col, "ERROR_CODE"] + (["SOURCE_REGION"] if drift else [])
    fn = f"backup_jobs_{d:%Y%m%d}.csv"
    with open(f"{OUT}/jobs/{fn}", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for j in rows:
            v = j["VM"]
            rec = [j["JOB_ID"], shown(v), rid(v), vault(v), "Daily-30d" if v["env"] in ("prod", "shared") else "Weekly-12w",
                   ts(j["START"]), ts(j["END"]), j["STATUS"], "" if j["SIZE"] is None else j["SIZE"], j["ERROR"]]
            w.writerow(rec + ([v["region"]] if drift else []))
            unique_jobs.add(j["JOB_ID"])
    if prev not in MISSING_DAYS:  # after a missing file the overlap rows are the only copy, not duplicates
        for j in overlap:
            add_answer("D06", fn, j["JOB_ID"], j["VM"]["name"], f"repeated from backup_jobs_{prev:%Y%m%d}.csv")
    if d in d09_days:
        add_answer("D09", fn, "", "", "timestamps written as dd.MM.yyyy HH:mm")
    file_rows[fn] = len(rows)
    raw_rows += len(rows)
add_answer("D07", f"backup_jobs_{DRIFT_FROM:%Y%m%d}.csv", "", "",
           "from this day: BACKUP_SIZE_GB renamed to BackupSizeGB, new column SOURCE_REGION")

# ---------------------------------------------------------------- storage (one file per month)
os.makedirs(f"{OUT}/storage", exist_ok=True)
storage_totals, orphan_totals, orphan_vms = {}, {}, set()
for month_start in (date(2026, 9, 1), date(2026, 10, 1)):
    month = f"{month_start:%Y-%m}"
    fn = f"backup_storage_{month_start:%Y%m}.csv"
    total = orphan = 0.0
    with open(f"{OUT}/storage/{fn}", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MONTH", "VM_NAME", "VM_RESOURCE_ID", "VAULT_NAME", "PROTECTION_STATE", "RETAINED_STORAGE_GB", "STORAGE_COST_USD"])
        for v in vms:
            if not v["protected"]:
                continue
            stopped = v["deleted"] is not None and v["deleted"] <= month_start
            gb = v["base_gb"] if not stopped else round(v["base_gb"] * 0.9, 0)
            cost = round(gb * GB_PRICE, 2)
            w.writerow([month, shown(v), rid(v), vault(v), "ProtectionStopped" if stopped else "Protected", gb, f"{cost:.2f}"])
            total += cost
            if stopped:
                orphan += cost
                orphan_vms.add(v["name"])
                add_answer("D01", fn, f"{v['name']}|{month}", v["name"],
                           f"VM deleted {v['deleted']}; backup storage still billed {cost:.2f} USD")
    storage_totals[month], orphan_totals[month] = round(total, 2), round(orphan, 2)

# ---------------------------------------------------------------- answer key + expected totals
os.makedirs(f"{OUT}/answer-keys/vm-backups", exist_ok=True)
with open(f"{OUT}/answer-keys/vm-backups/answer_key.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["defect_id", "file_name", "record_id", "vm_name", "detail"])
    w.writeheader()
    w.writerows(answer)
counts = {}
for a in answer:
    counts[a["defect_id"]] = counts.get(a["defect_id"], 0) + 1
expected = {
    "seed": SEED, "period": [str(START), str(END)],
    "vms_total": len(vms), "vms_protected": sum(v["protected"] for v in vms),
    "vms_not_in_cmdb": [v["name"] for v in vms if not v["in_cmdb"]],
    "job_files": len(file_rows), "job_rows_raw": raw_rows, "job_ids_unique": len(unique_jobs),
    "job_rows_duplicate_overlap": raw_rows - len(unique_jobs),
    "missing_job_files": sorted(f"backup_jobs_{d:%Y%m%d}.csv" for d in MISSING_DAYS),
    "storage_rows": {m: sum(1 for v in vms if v["protected"]) for m in storage_totals},
    "storage_cost_usd": storage_totals, "orphaned_backup_cost_usd": orphan_totals,
    "orphaned_vms": sorted(orphan_vms), "unprotected_prod_vms": sorted(failing),
    "defect_counts": counts,
}
with open(f"{OUT}/answer-keys/vm-backups/expected_totals.json", "w", encoding="utf-8") as f:
    json.dump(expected, f, indent=2)
print(json.dumps({k: expected[k] for k in ("vms_total", "vms_protected", "job_files", "job_rows_raw", "job_ids_unique",
                                           "storage_cost_usd", "orphaned_backup_cost_usd", "defect_counts")}, indent=1))
