"""Legacy Defuse Kit | Zontique Corp legacy data generator (bad on purpose).

One shared truth (organisation, applications, cloud scopes, resources, VMs) feeds all five
legacy systems, so they join. Every planted defect is written to an answer key.

Period: cloud cost 2024-01-01 .. 2026-10-31, one export file per provider and month (as of 2026-10-31);
budget FY2024..FY2026; VM Backups keep the last 4 months (2026-07 .. 2026-10). USD.

Output (<out>/):
  snowflake/cost/focus_<provider>_<yyyymm>.csv   FOCUS 1.2 cost exports, Azure + Google Cloud, daily lines
  snowflake/cmdb/assets.csv, asset_groups.csv     Cloud CMDB
  snowflake/fin/financial_budget.csv              Financial system (budget per cost center and month)
  azure_sql/*.csv + license_load.sql              License & Inventory (Azure SQL, dbo)
  vm_backups/jobs/*.csv, storage/*.csv            VM Backups daily / monthly exports
  answer-keys/<system>/answer_key.csv, expected_totals.json, answer-keys/expected_gold.json

Usage: python generate_zontique.py <out folder> [--seed 42]   (same seed = identical files)
"""
import csv, json, os, random, sys, uuid
from collections import defaultdict
from datetime import date, datetime, timedelta

args = [a for a in sys.argv[1:] if not a.startswith("--")]
OUT = args[0] if args else "out"
SEED = int(sys.argv[sys.argv.index("--seed") + 1]) if "--seed" in sys.argv else 42
rnd = random.Random(SEED)
START, END = date(2024, 1, 1), date(2026, 10, 31)
DAYS = [START + timedelta(d) for d in range((END - START).days + 1)]
MONTHS = sorted({d.replace(day=1) for d in DAYS})
RECENT = date(2026, 7, 1)                       # start of the "current" period (backups, growth, planted events)
BK_DAYS = [d for d in DAYS if d >= RECENT]
BK_MONTHS = [m for m in MONTHS if m >= RECENT]
FISCAL_YEARS = (2024, 2025, 2026)


def rdate(a, b):
    return a + timedelta(rnd.randint(0, (b - a).days))


def mkey(d):
    return f"{d:%Y-%m}"


def uid():
    return str(uuid.UUID(int=rnd.getrandbits(128)))


def hexid(n=4):
    return "".join(rnd.choice("0123456789abcdef") for _ in range(n))


# ======================================================================= answer keys
AK = defaultdict(list)
AK_COLS = ["defect_id", "table_name", "record_id", "field", "delivered_value", "expected_value", "expected_action", "detail"]


def ak(system, defect, table, record, field="", delivered="", expected="", action="", detail=""):
    AK[system].append(dict(defect_id=defect, table_name=table, record_id=record, field=field,
                           delivered_value=delivered, expected_value=expected, expected_action=action, detail=detail))


# ======================================================================= organisation (finance hierarchy)
ORG = [  # cc, cc name, dept code, dept name, business code, business name, BU code, BU name, owner id
    ("0410", "Plant Operations - Riverside", "DEP-PLANT", "Plant Operations", "MFG-IC", "Industrial Components", "MFG", "Manufacturing", "EMP-1001"),
    ("0420", "Plant Operations - Lakeside", "DEP-PLANT", "Plant Operations", "MFG-IC", "Industrial Components", "MFG", "Manufacturing", "EMP-1002"),
    ("0450", "Quality Control", "DEP-QC", "Quality Control", "MFG-IC", "Industrial Components", "MFG", "Manufacturing", "EMP-1003"),
    ("0610", "Supply Chain Planning", "DEP-SCM", "Supply Chain", "MFG-SCL", "Supply Chain & Logistics", "MFG", "Manufacturing", "EMP-1004"),
    ("0650", "Procurement", "DEP-PROC", "Procurement", "MFG-SCL", "Supply Chain & Logistics", "MFG", "Manufacturing", "EMP-1005"),
    ("2110", "Sales", "DEP-SALES", "Sales", "COM-SAL", "Sales & Service", "COM", "Commercial", "EMP-2001"),
    ("2150", "Customer Service", "DEP-CS", "Customer Service", "COM-SAL", "Sales & Service", "COM", "Commercial", "EMP-2002"),
    ("2310", "Marketing", "DEP-MKT", "Marketing", "COM-MKT", "Brand & Marketing", "COM", "Commercial", "EMP-2003"),
    ("3110", "IT Operations - Infrastructure", "DEP-ITOPS", "IT Operations", "CORP-TECH", "Technology", "CORP", "Corporate Services", "EMP-3001"),
    ("3120", "IT Operations - End User Services", "DEP-ITOPS", "IT Operations", "CORP-TECH", "Technology", "CORP", "Corporate Services", "EMP-3002"),
    ("3150", "Platform Engineering", "DEP-PLAT", "Platform Engineering", "CORP-TECH", "Technology", "CORP", "Corporate Services", "EMP-3003"),
    ("3170", "Data & Analytics", "DEP-DATA", "Data & Analytics", "CORP-TECH", "Technology", "CORP", "Corporate Services", "EMP-3004"),
    ("3510", "Finance", "DEP-FIN", "Finance", "CORP-FUN", "Corporate Functions", "CORP", "Corporate Services", "EMP-3005"),
    ("3550", "Human Resources", "DEP-HR", "Human Resources", "CORP-FUN", "Corporate Functions", "CORP", "Corporate Services", "EMP-3006"),
    ("3590", "Legal & Compliance", "DEP-LEGAL", "Legal & Compliance", "CORP-FUN", "Corporate Functions", "CORP", "Corporate Services", "EMP-3007"),
]
CC = {o[0]: o for o in ORG}

# id, name, code, cost center, criticality, lifecycle, owner, spend weight
APPS = [
    ("APP-001", "Manufacturing Execution", "MFGEXE", "0410", "Business Critical", "Active", "Danielle Johnson", 2.0),
    ("APP-002", "Plant Maintenance", "PLMNT", "0420", "Administrative", "Active", "Jill Rhodes", 0.8),
    ("APP-003", "Quality Management", "QMSYS", "0450", "Mission Critical", "Active", "Anthony Davis", 1.4),
    ("APP-004", "Demand Planning", "DMDPLN", "0610", "Administrative", "Active", "Anna Baldwin", 1.2),
    ("APP-005", "Warehouse Management", "WMSYS", "0610", "Business Critical", "Active", "Bridget Pacheco", 1.3),
    ("APP-006", "Supplier Portal", "SUPPRT", "0650", "Business Operational", "Active", "Helen Peterson", 0.9),
    ("APP-007", "Sales CRM", "SLSCRM", "2110", "Business Operational", "Active", "Curtis James", 1.1),
    ("APP-008", "E-Commerce Storefront", "ECOMM", "2110", "Mission Critical", "Active", "Lindsay Blair", 3.0),
    ("APP-009", "Customer Support Desk", "CSDESK", "2150", "Business Operational", "Active", "Christian Carter", 0.8),
    ("APP-010", "Marketing Automation", "MKTAUT", "2310", "Business Operational", "Active", "Barbara Bush", 1.0),
    ("APP-011", "Brand Website", "BRWEB", "2310", "Administrative", "Active", "Tracie Wyatt", 0.7),
    ("APP-012", "IT Service Management", "ITSMGT", "3110", "Business Operational", "Active", "Cynthia Diaz", 0.9),
    ("APP-013", "Endpoint Management", "ENDPT", "3120", "Business Critical", "Active", "Jacqueline Sutton", 0.8),
    ("APP-014", "Shared Platform Services", "PLAT", "3150", "Business Critical", "Active", "Amanda Sanchez", 1.8),
    ("APP-015", "Enterprise Data Platform", "EDPLAT", "3170", "Mission Critical", "Active", "Gabrielle Cameron", 3.5),
    ("APP-016", "Billing", "BILL", "3510", "Business Operational", "Active", "Linda Morales", 1.0),
    ("APP-017", "General Ledger", "GLEDG", "3510", "Business Operational", "Retired", "Jason Porter", 0.6),
    ("APP-018", "HR Portal", "HRPORT", "3550", "Administrative", "Deprecated", "Donald Mcgee", 0.7),
    ("APP-019", "Contract Management", "CNTRCT", "3590", "Business Critical", "Deprecated", "Rachael Ross", 0.7),
    ("APP-020", "Legacy eDiscovery Archive", "EDISC", "3590", "Business Critical", "Retired", "William Davis", 0.8),
]
APP = {a[0]: a for a in APPS}
APP_BY_CODE = {a[2]: a for a in APPS}
GRP = {a[0]: f"GRP-{a[0][4:]}" for a in APPS}

# ======================================================================= cloud scopes (truth)
SUBS = {"prod": ("ae340454-cac5-468c-a8f4-9481a0a04dc4", "zontique-prod"),
        "test": ("62801c45-1043-4a10-98ae-43346c12ace8", "zontique-nonprod"),
        "dev": ("8da0365b-f898-47b9-805c-acec877409a9", "zontique-sandbox"),
        "shared": ("77d21e02-ff01-4f99-988c-24c961b1cd22", "zontique-shared")}
AZ_REGIONS = {"eastus2": "East US 2", "westeurope": "West Europe", "northeurope": "North Europe",
              "uksouth": "UK South", "centralus": "Central US", "southeastasia": "Southeast Asia"}
GCP_REGIONS = {"europe-west1": "Belgium", "us-central1": "Iowa", "us-east4": "Northern Virginia", "asia-southeast1": "Singapore"}
TIERS = ["web", "api", "app", "data", "sec", "mon", "net"]

scopes = []  # dict(provider, name, app, tier, env, region, sub_id, sub_name, in_cmdb, cmdb_status, cmdb_deleted)


def add_scope(provider, app_id, tier, env, region, name=None, in_cmdb=True):
    code = APP[app_id][2].lower() if app_id else None
    if provider == "Azure":
        name = name or f"rg-{code}-{tier}-{env}-{region}-001"
        sub_id, sub_name = SUBS[env]
    else:
        name = name or f"prj-{code}-{ {'prod': 'p', 'test': 'n', 'dev': 'd'}[env] }-{tier}-{hexid()}".replace(" ", "")
        sub_id, sub_name = name, name
    s = dict(provider=provider, name=name, app=app_id, tier=tier, env=env, region=region, sub_id=sub_id,
             sub_name=sub_name, in_cmdb=in_cmdb, cmdb_status="Active", cmdb_deleted=None,
             created=rdate(date(2023, 1, 1), date(2026, 3, 1)))   # billing starts when the scope is created
    scopes.append(s)
    return s


for a in APPS:
    app_id, code = a[0], a[2]
    n_prod = 4 if a[7] >= 2 else (3 if a[7] >= 1 else 2)
    prod_tiers = rnd.sample(TIERS, n_prod)
    if code in ("EDPLAT", "ECOMM") and "data" not in prod_tiers:
        prod_tiers[0] = "data"
    env_main = "shared" if code == "PLAT" else "prod"
    for t in prod_tiers:
        add_scope("Azure", app_id, t, env_main, rnd.choice(list(AZ_REGIONS)))
    for env in rnd.sample(["test", "dev"], rnd.choice([1, 1, 2])):
        add_scope("Azure", app_id, rnd.choice(TIERS[:6]), env, rnd.choice(list(AZ_REGIONS)))
GCP_APPS = ["MFGEXE", "PLMNT", "DMDPLN", "SUPPRT", "SLSCRM", "ECOMM", "MKTAUT", "BRWEB", "ITSMGT", "ENDPT",
            "PLAT", "EDPLAT", "BILL", "HRPORT", "CNTRCT", "EDISC"]
for code in GCP_APPS:
    for _ in range(2 if code in ("EDPLAT", "ECOMM", "BILL", "PLAT") else 1):
        add_scope("GCP", APP_BY_CODE[code][0], rnd.choice(["web", "api", "app", "data", "mon"]),
                  rnd.choice(["prod", "prod", "test", "dev"]), rnd.choice(list(GCP_REGIONS)))
# shadow IT: billed, never registered in the CMDB (cost C08, backups D02)
SHADOW = []
for app_code, tier, region in [("sapfi", "app", "westeurope"), ("sapfi", "data", "westeurope"),
                               ("legerp", "app", "northeurope"), ("legerp", "data", "northeurope"), ("ftpgw", "web", "eastus2")]:
    SHADOW.append(add_scope("Azure", None, tier, "prod", region, name=f"rg-{app_code}-{tier}-prod-{region}-001", in_cmdb=False))
for nm in ["prj-mlsbx-d-data-77a1", "prj-genai-poc-d-app-3c09"]:
    SHADOW.append(add_scope("GCP", None, "app", "dev", "us-central1", name=nm, in_cmdb=False))

# retired apps: CMDB says their scopes were deleted in spring, but resources keep billing (zombies)
ZOMBIE_SCOPES = []
for code in ("GLEDG", "EDISC"):
    own = [s for s in scopes if s["app"] == APP_BY_CODE[code][0] and s["provider"] == "Azure"]
    for s in own[:-1]:
        s["cmdb_status"], s["cmdb_deleted"] = "Deleted", date(2026, rnd.choice([4, 5, 6]), rnd.randint(1, 28))
        ZOMBIE_SCOPES.append(s)

# ======================================================================= resources (truth)
VM_SIZES = {"B2ms": (2, 0.0832), "D2s v5": (2, 0.096), "D4s v5": (4, 0.192), "D8s v5": (8, 0.384),
            "E8ds v5": (8, 0.576), "E16ds v5": (16, 1.152)}
GCE_SIZES = {"e2-standard-2": (2, 0.067), "e2-standard-4": (4, 0.134), "n2-standard-8": (8, 0.389)}
AZ_SVC = {  # tier -> list of (type, prefix, service, category, subcategory, daily range)
    "web": [("Microsoft.Web/serverfarms", "asp", "Azure App Service", "Compute", "App Service Plans", (6, 25))],
    "api": [("Microsoft.Web/sites", "func", "Functions", "Compute", "Serverless Compute", (1, 6))],
    "app": [],
    "data": [("Microsoft.Sql/servers/databases", "sqldb", "SQL Database", "Databases", "Relational Databases", (10, 60))],
    "sec": [("Microsoft.KeyVault/vaults", "kv", "Key Vault", "Security", "Secret Management", (0.1, 1.2))],
    "mon": [("Microsoft.OperationalInsights/workspaces", "log", "Log Analytics", "Management and Governance", "Observability", (3, 20))],
    "net": [("Microsoft.Network/azureFirewalls", "afw", "Azure Firewall", "Networking", "Network Security", (28, 32)),
            ("Microsoft.Network/publicIPAddresses", "pip", "Virtual Network", "Networking", "Network Infrastructure", (0.1, 0.2))],
}
GCP_SVC = {
    "web": [("run.googleapis.com/Service", "run", "Cloud Run", "Compute", "Serverless Compute", (1, 8))],
    "api": [("run.googleapis.com/Service", "run", "Cloud Run", "Compute", "Serverless Compute", (1, 8))],
    "app": [],
    "data": [("bigquery.googleapis.com/Dataset", "bq", "BigQuery", "Analytics", "Data Warehouses", (5, 50)),
             ("sqladmin.googleapis.com/Instance", "csql", "Cloud SQL", "Databases", "Relational Databases", (8, 40))],
    "mon": [("logging.googleapis.com/Bucket", "logs", "Cloud Logging", "Management and Governance", "Observability", (1, 6))],
}
PER_TIER_VMS = {"web": 2, "api": 2, "app": 2, "data": 2, "sec": 1, "mon": 1, "net": 0}

resources = []  # dict(id, name, type, service, category, subcat, scope, daily, vm_size, cores, payg, covered, deleted, ...)


def az_rid(s, rtype, name):
    if rtype == "Microsoft.Sql/servers/databases":
        return f"/subscriptions/{s['sub_id']}/resourceGroups/{s['name']}/providers/Microsoft.Sql/servers/sql-{name[6:]}/databases/{name}"
    return f"/subscriptions/{s['sub_id']}/resourceGroups/{s['name']}/providers/{rtype}/{name}"


for s in scopes:
    code = (APP[s["app"]][2] if s["app"] else s["name"].split("-")[1]).lower()
    weight = APP[s["app"]][7] if s["app"] else 1.2
    env_f = 1.0 if s["env"] in ("prod", "shared") else 0.35
    if s["provider"] == "Azure":
        svcs = list(AZ_SVC[s["tier"]]) + [("Microsoft.Storage/storageAccounts", "st", "Storage", "Storage", "Object Storage", (0.5, 8))]
        if code in ("edplat", "ecomm") and s["env"] == "prod" and s["tier"] in ("data", "app", "api"):
            svcs.append(("Microsoft.CognitiveServices/accounts", "oai", "Azure OpenAI", "AI and Machine Learning", "Generative AI", (15, 45)))
        for rtype, pre, svc, cat, sub, (lo, hi) in svcs:
            name = f"{pre}{code}{s['env']}{hexid(3)}" if pre == "st" else f"{pre}-{code}-{s['tier']}-{s['env']}"
            resources.append(dict(id=az_rid(s, rtype, name), name=name, type=rtype, service=svc, category=cat, subcat=sub,
                                  scope=s, daily=rnd.uniform(lo, hi) * weight * env_f, vm=False))
        n = PER_TIER_VMS.get(s["tier"], 1)
        if s["env"] in ("dev", "test"):
            n = min(n, 1)
        for i in range(1, n + 1):
            name = f"vm-{code}-{s['tier']}-{s['env']}-{i:02d}"
            size = rnd.choice(["D4s v5", "D8s v5", "E8ds v5", "E16ds v5"] if s["env"] in ("prod", "shared") else ["B2ms", "D2s v5", "D4s v5"])
            resources.append(dict(id=az_rid(s, "Microsoft.Compute/virtualMachines", name), name=name,
                                  type="Microsoft.Compute/virtualMachines", service="Virtual Machines", category="Compute",
                                  subcat="Virtual Machines", scope=s, vm=True, vm_size=size, cores=VM_SIZES[size][0],
                                  hourly=VM_SIZES[size][1], payg=False, covered=False, deleted=None))
    else:
        svcs = list(GCP_SVC[s["tier"]]) + [("storage.googleapis.com/Bucket", "gcs", "Cloud Storage", "Storage", "Object Storage", (0.5, 6))]
        for rtype, pre, svc, cat, sub, (lo, hi) in svcs:
            name = f"{pre}-{code}-{s['tier']}-{hexid(4)}"
            rid = f"//{rtype.split('/')[0]}/projects/{s['name']}/{rtype.split('/')[1].lower()}s/{name}"
            resources.append(dict(id=rid, name=name, type=rtype, service=svc, category=cat, subcat=sub, scope=s,
                                  daily=rnd.uniform(lo, hi) * weight * env_f, vm=False))
        if s["tier"] in ("app", "web", "api"):
            size = rnd.choice(list(GCE_SIZES))
            name = f"gce-{code}-{s['tier']}-{hexid(4)}"
            zone = s["region"] + "-b"
            resources.append(dict(id=f"//compute.googleapis.com/projects/{s['name']}/zones/{zone}/instances/{name}", name=name,
                                  type="compute.googleapis.com/Instance", service="Compute Engine", category="Compute",
                                  subcat="Virtual Machines", scope=s, vm=True, vm_size=size, cores=GCE_SIZES[size][0],
                                  hourly=GCE_SIZES[size][1], payg=False, covered=False, deleted=None))

az_vms = [r for r in resources if r["vm"] and r["scope"]["provider"] == "Azure"]
for v in az_vms:  # Windows licence: pay-as-you-go on ~35%, reservations on ~30% of production VMs
    v["payg"] = rnd.random() < 0.35
    v["covered"] = v["scope"]["env"] in ("prod", "shared") and v["vm_size"] in ("D4s v5", "D8s v5") and rnd.random() < 0.6
# 4 production VMs really deleted during the period (cost stops; backups keep storage = orphan)
cand = [v for v in az_vms if v["scope"]["env"] == "prod" and v["scope"]["in_cmdb"] and not v["covered"]
        and v["scope"] not in ZOMBIE_SCOPES]
for v, d in zip(rnd.sample(cand, 4), [date(2026, 7, 22), date(2026, 8, 18), date(2026, 9, 9), date(2026, 10, 6)]):
    v["deleted"] = d
for r in resources:
    r.setdefault("deleted", None)

# ======================================================================= FOCUS 1.2 cost lines (truth)
FOCUS_COLS = ["AvailabilityZone", "BilledCost", "BillingAccountId", "BillingAccountName", "BillingAccountType", "BillingCurrency",
              "BillingPeriodEnd", "BillingPeriodStart", "CapacityReservationId", "CapacityReservationStatus", "ChargeCategory",
              "ChargeClass", "ChargeDescription", "ChargeFrequency", "ChargePeriodEnd", "ChargePeriodStart",
              "CommitmentDiscountCategory", "CommitmentDiscountId", "CommitmentDiscountName", "CommitmentDiscountQuantity",
              "CommitmentDiscountStatus", "CommitmentDiscountType", "CommitmentDiscountUnit", "ConsumedQuantity", "ConsumedUnit",
              "ContractedCost", "ContractedUnitPrice", "EffectiveCost", "InvoiceId", "InvoiceIssuerName", "ListCost", "ListUnitPrice",
              "PricingCategory", "PricingCurrency", "PricingCurrencyContractedUnitPrice", "PricingCurrencyEffectiveCost",
              "PricingCurrencyListUnitPrice", "PricingQuantity", "PricingUnit", "ProviderName", "PublisherName", "RegionId",
              "RegionName", "ResourceId", "ResourceName", "ResourceType", "ServiceCategory", "ServiceName", "ServiceSubcategory",
              "SkuId", "SkuMeter", "SkuPriceDetails", "SkuPriceId", "SubAccountId", "SubAccountName", "SubAccountType", "Tags",
              "x_ResourceGroupName", "x_ExportRunId"]
BILLING = {"Azure": ("ZQ-EA-8841023", "Zontique Enterprise Agreement", "Billing Profile", "Microsoft", "Subscription"),
           "GCP": ("01A7C3-5D2E9B-F40C18", "Zontique GCP Billing", "Billing Account", "Google Cloud", "Project")}
EA_DISCOUNT = {"Azure": 0.10, "GCP": 0.05}
RI = {"D4s v5": (f"/providers/Microsoft.Capacity/reservationOrders/{uid()}/reservations/{uid()}", "RI-D4s-v5-3yr-eastus2"),
      "D8s v5": (f"/providers/Microsoft.Capacity/reservationOrders/{uid()}/reservations/{uid()}", "RI-D8s-v5-3yr-eastus2")}


def iso(d):
    return f"{d:%Y-%m-%d}T00:00:00Z"


def next_month(d):
    return date(d.year + d.month // 12, d.month % 12 + 1, 1)


def r6(x):
    return round(x, 6)


def tags_for(r):
    s = r["scope"]
    if not s["app"]:
        return {"environment": s["env"]} if rnd.random() < 0.5 else {}
    a = APP[s["app"]]
    code = a[2].lower() if s["provider"] == "GCP" else a[2]  # GCP labels are lower case by design
    return {"application": code, "cost-center": a[3], "environment": s["env"]}


for r in resources:
    r["tags"] = tags_for(r)
    r["sku"] = f"{hexid(4).upper()}{hexid(4).upper()}/{rnd.randint(1, 99):04d}"


def base_row(r, d):
    s = r["scope"]
    p = s["provider"]
    ba = BILLING[p]
    regs = AZ_REGIONS if p == "Azure" else GCP_REGIONS
    return dict(BillingAccountId=ba[0], BillingAccountName=ba[1], BillingAccountType=ba[2], BillingCurrency="USD",
                BillingPeriodStart=iso(d.replace(day=1)), BillingPeriodEnd=iso(next_month(d)),
                ChargePeriodStart=iso(d), ChargePeriodEnd=iso(d + timedelta(1)), InvoiceIssuerName=ba[3],
                ProviderName=ba[3], PublisherName=ba[3], PricingCurrency="USD", RegionId=s["region"],
                RegionName=regs[s["region"]], ResourceId=r["id"], ResourceName=r["name"], ResourceType=r["type"],
                ServiceCategory=r["category"], ServiceName=r["service"], ServiceSubcategory=r["subcat"],
                SubAccountId=s["sub_id"], SubAccountName=s["sub_name"], SubAccountType=ba[4],
                Tags=json.dumps(r["tags"], separators=(",", ":")) if r["tags"] else "",
                x_ResourceGroupName=s["name"] if p == "Azure" else "", ChargeCategory="Usage",
                ChargeFrequency="Usage-Based", PricingCategory="Standard", SkuId=r["sku"],
                SkuPriceId=f"{r['sku']}/{hexid(6)}", InvoiceId=f"{'E' if p == 'Azure' else 'G'}{d:%Y%m}{hexid(4).upper()}")


def priced(row, qty, unit, list_unit, p, consumed_unit=None):
    contracted = list_unit * (1 - EA_DISCOUNT[p])
    row.update(ConsumedQuantity=r6(qty), ConsumedUnit=consumed_unit or unit, PricingQuantity=r6(qty), PricingUnit=unit,
               ListUnitPrice=r6(list_unit), ListCost=r6(qty * list_unit), ContractedUnitPrice=r6(contracted),
               ContractedCost=r6(qty * contracted), BilledCost=r6(qty * contracted), EffectiveCost=r6(qty * contracted),
               PricingCurrencyListUnitPrice=r6(list_unit), PricingCurrencyContractedUnitPrice=r6(contracted),
               PricingCurrencyEffectiveCost=r6(qty * contracted))
    return row


truth_lines = []   # (provider, day, row dict, resource)
ri_month = defaultdict(float)
for r in resources:
    s = r["scope"]
    p = s["provider"]
    for i, d in enumerate(DAYS):
        if d < s["created"]:
            continue
        if r["deleted"] and d >= r["deleted"]:
            break
        growth = (0.75 + 0.25 * i / (RECENT - START).days) if d < RECENT else 1 + 0.0025 * (d - RECENT).days
        if r["vm"]:
            nonprod = s["env"] in ("dev", "test")
            hours = (0 if d.weekday() >= 5 else 12) if nonprod else 24
            if hours == 0:
                continue
            row = base_row(r, d)
            row.update(ChargeDescription=f"{r['vm_size']} {'Windows' if p == 'Azure' else 'Linux'}", SkuMeter=r["vm_size"],
                       SkuPriceDetails=json.dumps({"CoreCount": r["cores"]}, separators=(",", ":")))
            priced(row, hours, "1 Hour", r["hourly"], p, "Hours")
            if r["covered"]:
                eff = r6(hours * r["hourly"] * 0.58)
                cdi, cdn = RI[r["vm_size"]]
                row.update(BilledCost=0.0, EffectiveCost=eff, PricingCurrencyEffectiveCost=eff, PricingCategory="Committed",
                           CommitmentDiscountId=cdi, CommitmentDiscountName=cdn, CommitmentDiscountCategory="Usage",
                           CommitmentDiscountType="Reservation", CommitmentDiscountStatus="Used",
                           CommitmentDiscountQuantity=r6(hours), CommitmentDiscountUnit="Hours")
                ri_month[(mkey(d), r["vm_size"])] += eff
            truth_lines.append((p, d, row, r))
            if r.get("payg"):
                lic = base_row(r, d)
                lic.update(ServiceName="Virtual Machines Licenses", ServiceSubcategory="Software Licenses",
                           ChargeDescription="Windows Server license (pay-as-you-go)", SkuMeter="Windows Server",
                           SkuId=f"{r['sku']}-WIN", SkuPriceId=f"{r['sku']}-WIN/{hexid(6)}",
                           SkuPriceDetails=json.dumps({"CoreCount": r["cores"]}, separators=(",", ":")))
                priced(lic, hours * r["cores"], "1 Core Hour", 0.0115, p, "Core Hours")
                truth_lines.append((p, d, lic, r))
        else:
            daily = r["daily"] * growth * (0.8 if d.weekday() >= 5 and r["category"] == "Compute" else 1.0)
            if r["service"] == "Azure OpenAI" and "edplat" in r["name"] and d >= date(2026, 9, 1):
                daily *= 1.9  # AI adoption spike from September (Enterprise Data Platform overrun story)
            daily *= rnd.uniform(0.9, 1.1)
            row = base_row(r, d)
            unit_price = {"Storage": 0.0208, "Cloud Storage": 0.020}.get(r["service"], 1.0)
            unit = "1 GB/Month" if unit_price < 1 else "1 Unit"
            row.update(ChargeDescription=f"{r['service']} usage", SkuMeter=f"{r['service']} {r['subcat']}", SkuPriceDetails="{}")
            priced(row, daily / (unit_price * (1 - EA_DISCOUNT[p])), unit, unit_price, p,
                   "GB" if unit_price < 1 else "Units")
            truth_lines.append((p, d, row, r))

# reservation purchases (monthly, no resource), one credit, = FOCUS facts, not defects
extra_lines = []
for (m, size), amt in sorted(ri_month.items()):
    d = date.fromisoformat(m + "-01")
    cdi, cdn = RI[size]
    row = dict(BillingAccountId=BILLING["Azure"][0], BillingAccountName=BILLING["Azure"][1], BillingAccountType="Billing Profile",
               BillingCurrency="USD", BillingPeriodStart=iso(d), BillingPeriodEnd=iso(next_month(d)), ChargePeriodStart=iso(d),
               ChargePeriodEnd=iso(next_month(d)), ChargeCategory="Purchase", ChargeFrequency="Recurring",
               ChargeDescription=f"Reservation {cdn} monthly payment", BilledCost=r6(amt), EffectiveCost=0.0, ListCost=r6(amt),
               ContractedCost=r6(amt), PricingCategory="Committed", CommitmentDiscountId=cdi, CommitmentDiscountName=cdn,
               CommitmentDiscountCategory="Usage", CommitmentDiscountType="Reservation", InvoiceIssuerName="Microsoft",
               ProviderName="Microsoft", PublisherName="Microsoft", PricingCurrency="USD", ServiceCategory="Compute",
               ServiceName="Virtual Machines", ServiceSubcategory="Virtual Machines", SubAccountId=SUBS["prod"][0],
               SubAccountName=SUBS["prod"][1], SubAccountType="Subscription", InvoiceId=f"E{d:%Y%m}RI01",
               PricingCurrencyEffectiveCost=0.0, PricingQuantity=1, PricingUnit="1 Month", SkuMeter=size)
    extra_lines.append(("Azure", d, row, None))
d = date(2026, 9, 1)
extra_lines.append(("Azure", d, dict(BillingAccountId=BILLING["Azure"][0], BillingAccountName=BILLING["Azure"][1],
                                     BillingAccountType="Billing Profile", BillingCurrency="USD", BillingPeriodStart=iso(d),
                                     BillingPeriodEnd=iso(next_month(d)), ChargePeriodStart=iso(d), ChargePeriodEnd=iso(next_month(d)),
                                     ChargeCategory="Credit", ChargeFrequency="One-Time", ChargeDescription="Azure Hybrid Benefit migration credit",
                                     BilledCost=-1500.0, EffectiveCost=-1500.0, InvoiceIssuerName="Microsoft", ProviderName="Microsoft",
                                     PublisherName="Microsoft", PricingCurrency="USD", SubAccountId=SUBS["shared"][0],
                                     SubAccountName=SUBS["shared"][1], SubAccountType="Subscription", InvoiceId="E202609CR01",
                                     PricingCurrencyEffectiveCost=-1500.0), None))

# ======================================================================= cost defects (applied per resource, like real tags)
cmdb_scopes = [s for s in scopes if s["in_cmdb"]]
tagged = [r for r in resources if r["scope"]["app"]]
pool = list(tagged)
rnd.shuffle(pool)


def take(frac, flt=lambda r: True):
    global pool
    n = round(len([r for r in tagged if flt(r)]) * frac)
    got = [r for r in pool if flt(r)][:n]
    pool = [r for r in pool if r not in got]
    return got


for r in take(0.12):   # C03 application tag missing
    old = dict(r["tags"])
    r["tags"].pop("application", None)
    ak("cost", "C03", "FOCUS_COST", r["id"], "Tags.application", "", old["application"].upper(), "Fix",
       "application tag missing: derive from resource group / project -> CMDB")
for r in take(0.06):   # C02 tag key variant
    key = rnd.choice(["Application", "APP", "app_code"])
    r["tags"] = {(key if k == "application" else k): v for k, v in r["tags"].items()}
    ak("cost", "C02", "FOCUS_COST", r["id"], "Tags", key, "application", "Fix", f"tag key '{key}' instead of 'application'")
for r in take(0.05):   # C04 application value variant
    a = APP[r["scope"]["app"]]
    v = rnd.choice([a[1], a[2][:3] + "-" + a[2][3:], a[2] + "-PROD", a[2][:-1], a[1].upper()])
    r["tags"]["application"] = v
    ak("cost", "C04", "FOCUS_COST", r["id"], "Tags.application", v, a[2], "AI-classify",
       "application tag value is a name / typo, not the CMDB application code")
for r in take(0.03):   # C05 cost-center tag stale
    a = APP[r["scope"]["app"]]
    wrong = rnd.choice([c for c in CC if c != a[3]])
    r["tags"]["cost-center"] = wrong
    ak("cost", "C05", "FOCUS_COST", r["id"], "Tags.cost-center", wrong, a[3], "Flag",
       "cost-center tag disagrees with the CMDB application cost center (CMDB wins)")
gcp_res = [r for r in resources if r["scope"]["provider"] == "GCP"]
SVC_VARIANT = {"Compute Engine": "GCE", "Cloud Storage": "cloud storage", "BigQuery": "Big Query", "Cloud SQL": "CloudSQL",
               "Cloud Run": "cloud-run", "Cloud Logging": "Stackdriver Logging"}
for r in rnd.sample(gcp_res, round(len(gcp_res) * 0.15)):   # C06
    r["svc_variant"] = SVC_VARIANT[r["service"]]
    ak("cost", "C06", "FOCUS_COST", r["id"], "ServiceName/ServiceCategory", f"{r['svc_variant']} / (empty)",
       f"{r['service']} / {r['category']}", "AI-classify", "non-standard service name, ServiceCategory empty")
az_res = [r for r in resources if r["scope"]["provider"] == "Azure" and r["scope"]["in_cmdb"]]
rg_variant_scopes = rnd.sample([s for s in cmdb_scopes if s["provider"] == "Azure"], 3)
for s in rg_variant_scopes:   # C10
    ak("cost", "C10", "FOCUS_COST", s["name"], "x_ResourceGroupName", s["name"].upper(), s["name"], "Fix",
       "resource group name upper-cased in x_ResourceGroupName for every line of this group")
for s in ZOMBIE_SCOPES:   # C07
    ak("cost", "C07", "FOCUS_COST", s["name"], "", "", "", "Flag",
       f"spend continues after the CMDB marked the resource group Deleted on {s['cmdb_deleted']}")
for s in SHADOW:   # C08
    ak("cost", "C08", "FOCUS_COST", s["name"], "", "", "Unallocated", "Flag", "scope not in the CMDB: no owner, cost unallocated")
for r in resources:
    if r["vm"] and r["deleted"]:
        ak("cost", "FACT", "FOCUS_COST", r["id"], "", "", "", "None", f"VM deleted {r['deleted']}: cost lines stop (check backups)")
RENAME_FROM = date(2026, 9, 1)
ak("cost", "C12", "FOCUS_COST", SUBS["test"][0], "SubAccountName", "Zontique Non-Production", SUBS["test"][1], "Fix",
   "subscription renamed from 2026-09-01; join on SubAccountId, not the name")
DUP_DAY = date(2026, 8, 15)
GAP_DAY, GAP_PROJECTS = date(2026, 9, 14), None
GCP_TS_MONTH = "2026-10"


def deliver(p, d, row, r):
    row = {c: row.get(c, "") for c in FOCUS_COLS}
    for c in FOCUS_COLS:
        if row[c] is None:
            row[c] = ""
    if r is not None:
        if r["tags"] is not None:
            row["Tags"] = json.dumps(r["tags"], separators=(",", ":")) if r["tags"] else ""
        if r.get("svc_variant"):
            row["ServiceName"], row["ServiceCategory"] = r["svc_variant"], ""
        if r["scope"] in rg_variant_scopes:
            row["x_ResourceGroupName"] = r["scope"]["name"].upper()
    if p == "Azure" and row["SubAccountId"] == SUBS["test"][0] and d >= RENAME_FROM:
        row["SubAccountName"] = "Zontique Non-Production"
    if p == "GCP" and mkey(d) == GCP_TS_MONTH:
        for c in ("ChargePeriodStart", "ChargePeriodEnd", "BillingPeriodStart", "BillingPeriodEnd"):
            row[c] = datetime.strptime(row[c], "%Y-%m-%dT%H:%M:%SZ").strftime("%m/%d/%Y %H:%M")
    return row


os.makedirs(f"{OUT}/snowflake/cost", exist_ok=True)
files = defaultdict(list)
delivered_totals = defaultdict(lambda: [0, 0.0, 0.0])
gap_lines = [0, 0.0]
for p, d, row, r in truth_lines + extra_lines:
    if p == "GCP" and d == GAP_DAY:
        gap_lines[0] += 1
        gap_lines[1] += row["BilledCost"]
        continue
    run = f"{p.lower()}-{d:%Y%m}-r1"
    out = deliver(p, d, dict(row, x_ExportRunId=run), r)
    files[f"focus_{'azure' if p == 'Azure' else 'gcp'}_{d:%Y%m}.csv"].append(out)
    if p == "Azure" and d == DUP_DAY:
        files["focus_azure_202608_rerun_20260815.csv"].append(dict(out, x_ExportRunId="azure-202608-r2"))
ak("cost", "C01", "FOCUS_COST", "focus_azure_202608_rerun_20260815.csv", "", "", "", "Merge",
   f"export re-run: every line of {DUP_DAY} delivered a second time with a new x_ExportRunId")
ak("cost", "C09", "FOCUS_COST", f"GCP|{GAP_DAY}", "", "", "", "Flag",
   f"no Google Cloud lines for {GAP_DAY} (completeness gap, about {gap_lines[1]:.2f} USD)")
ak("cost", "C11", "FOCUS_COST", f"focus_gcp_{GCP_TS_MONTH.replace('-', '')}.csv", "ChargePeriodStart", "MM/dd/yyyy HH:mm",
   "yyyy-MM-ddTHH:mm:ssZ", "Fix", "date columns in a different format in this file")
for fn, rows in sorted(files.items()):
    with open(f"{OUT}/snowflake/cost/{fn}", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FOCUS_COLS)
        w.writeheader()
        w.writerows(rows)

# ======================================================================= CMDB (assets.csv, asset_groups.csv)
ASSET_COLS = ["asset_id", "asset_name", "asset_type", "asset_group_id", "status", "created_date", "last_modified_utc",
              "cloud_provider", "cloud_account_id", "cloud_account_name", "cloud_native_id", "environment", "application_code",
              "department", "cost_center", "owner_name", "owner_email", "criticality", "deleted_date", "mapping_last_verified"]


def ts_between(a, b):
    t = datetime(a.year, a.month, a.day) + timedelta(seconds=rnd.randint(0, int((b - a).total_seconds())))
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


assets = []
for a in APPS:
    first, last = a[6].split()[0].lower(), a[6].split()[1].lower()
    assets.append(dict(asset_id=a[0], asset_name=a[1], asset_type="BusinessApplication", asset_group_id=GRP[a[0]], status=a[5],
                       created_date=str(rdate(date(2023, 1, 1), date(2026, 3, 1))),
                       last_modified_utc=ts_between(date(2025, 6, 1), date(2026, 10, 30)), application_code=a[2],
                       department=CC[a[3]][3], cost_center=a[3], owner_name=a[6], owner_email=f"{first}.{last}@zontique.com",
                       criticality=a[4], deleted_date="", mapping_last_verified=""))
scope_rows = []
for s in cmdb_scopes:
    created = s["created"]
    row = dict(asset_name=s["name"], asset_type="AzureResourceGroup" if s["provider"] == "Azure" else "GcpProject",
               asset_group_id=GRP[s["app"]], status=s["cmdb_status"], created_date=str(created),
               last_modified_utc=ts_between(date(2025, 8, 1), date(2026, 10, 30)), cloud_provider=s["provider"],
               cloud_account_id=s["sub_id"] if s["provider"] == "Azure" else "",
               cloud_account_name=s["sub_name"] if s["provider"] == "Azure" else "",
               cloud_native_id=(f"/subscriptions/{s['sub_id']}/resourceGroups/{s['name']}" if s["provider"] == "Azure"
                                else f"projects/{s['name']}"), environment=s["env"],
               deleted_date=str(s["cmdb_deleted"]) if s["cmdb_deleted"] else "",
               mapping_last_verified=str(rdate(date(2025, 5, 1), date(2026, 10, 15))))
    row["_scope"] = s
    scope_rows.append(row)
rnd.shuffle(scope_rows)
for i, row in enumerate(scope_rows, 1):
    row["asset_id"] = f"AST-{i:04d}"

# M01 duplicate application (upper-case name, new id)
dup = dict(assets[11], asset_id="APP-021", asset_name=assets[11]["asset_name"].upper(),
           last_modified_utc=ts_between(date(2026, 1, 1), date(2026, 10, 30)))
assets.append(dup)
ak("cmdb", "M01", "ASSETS", "APP-021", "asset_name", dup["asset_name"], "APP-012", "Merge", "duplicate application, upper-case name")
# M02 department variants
for app_id, v in [("APP-010", "Mktg"), ("APP-006", "Procurement Dept"), ("APP-015", "Data and Analytics"), ("APP-013", "IT Ops")]:
    row = next(x for x in assets if x["asset_id"] == app_id)
    ak("cmdb", "M02", "ASSETS", app_id, "department", v, row["department"], "AI-classify", "department name not the finance name")
    row["department"] = v
# M11 cost center lost its leading zero
for app_id in ("APP-002", "APP-005"):
    row = next(x for x in assets if x["asset_id"] == app_id)
    ak("cmdb", "M11", "ASSETS", app_id, "cost_center", row["cost_center"].lstrip("0"), row["cost_center"], "Fix",
       "leading zero lost (file opened in Excel)")
    row["cost_center"] = row["cost_center"].lstrip("0")
# M09 owner email missing / malformed
for app_id, v in [("APP-015", ""), ("APP-009", "christian.carter(at)zontique.com")]:
    row = next(x for x in assets if x["asset_id"] == app_id)
    ak("cmdb", "M09", "ASSETS", app_id, "owner_email", v, row["owner_email"], "Flag", "owner email missing or malformed (PII)")
    row["owner_email"] = v
# M07 retired / deprecated application still owns active scopes
for a in APPS:
    if a[5] in ("Retired", "Deprecated"):
        act = [r for r in scope_rows if r["_scope"]["app"] == a[0] and r["status"] == "Active"]
        if act:
            ak("cmdb", "M07", "ASSETS", a[0], "status", a[5], "", "Flag",
               f"{a[5]} application still owns {len(act)} active scope(s): " + ", ".join(r["asset_name"] for r in act))
# M08 scopes deleted in the CMDB that still bill
for r in scope_rows:
    if r["_scope"] in ZOMBIE_SCOPES:
        ak("cmdb", "M08", "ASSETS", r["asset_id"], "status", "Deleted", "Active", "Flag",
           f"{r['asset_name']} marked Deleted on {r['deleted_date']} but still has cost lines")
pick = rnd.sample([r for r in scope_rows if r["_scope"] not in ZOMBIE_SCOPES], 4 + 5 + 14)
# M05 scope not mapped to any group
for r in pick[:4]:
    ak("cmdb", "M05", "ASSETS", r["asset_id"], "asset_group_id", "", r["asset_group_id"], "AI-extract",
       f"no group: the application code is in the name {r['asset_name']}")
    r["asset_group_id"] = ""
# M06 stale mapping: group of another application
for r in pick[4:9]:
    wrong = rnd.choice([g for g in GRP.values() if g != r["asset_group_id"]])
    ak("cmdb", "M06", "ASSETS", r["asset_id"], "asset_group_id", wrong, r["asset_group_id"], "Flag",
       f"stale mapping: the name {r['asset_name']} points to another application")
    r["asset_group_id"] = wrong
# M10 mapping not verified for 18 months
for r in pick[9:]:
    old = str(rdate(date(2023, 1, 1), date(2025, 4, 30)))
    ak("cmdb", "M10", "ASSETS", r["asset_id"], "mapping_last_verified", old, "", "Flag", "mapping older than 18 months")
    r["mapping_last_verified"] = old
# M04 duplicate scopes with an upper-case name
for r in rnd.sample([r for r in scope_rows if r["cloud_provider"] == "Azure" and r["asset_group_id"]], 3):
    d2 = dict(r, asset_id=f"AST-{len(scope_rows) + 1:04d}", asset_name=r["asset_name"].upper(),
              last_modified_utc=ts_between(date(2026, 1, 1), date(2026, 10, 30)))
    scope_rows.append(d2)
    ak("cmdb", "M04", "ASSETS", d2["asset_id"], "asset_name", d2["asset_name"], r["asset_id"], "Merge", "duplicate scope, upper-case name")
all_assets = assets + scope_rows
# M03 mixed date formats
for r in rnd.sample(all_assets, round(len(all_assets) * 0.05)):
    d0 = date.fromisoformat(r["created_date"])
    v = rnd.choice([f"{d0:%d/%m/%Y}", f"{d0:%m/%d/%Y}", "N/A"])
    ak("cmdb", "M03", "ASSETS", r["asset_id"], "created_date", v, r["created_date"], "Fix", "date not in yyyy-MM-dd")
    r["created_date"] = v
# M12 timestamp offset variant
for r in rnd.sample(all_assets, round(len(all_assets) * 0.10)):
    r["last_modified_utc"] = r["last_modified_utc"].replace("Z", "+00:00")
ak("cmdb", "M12", "ASSETS", "*", "last_modified_utc", "+00:00", "Z", "Fix", "about 10% of timestamps use +00:00 instead of Z")
os.makedirs(f"{OUT}/snowflake/cmdb", exist_ok=True)
with open(f"{OUT}/snowflake/cmdb/assets.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=ASSET_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(sorted(all_assets, key=lambda r: r["asset_id"]))
groups = [dict(asset_group_id=GRP[a[0]], asset_group_name=a[1], application_asset_id=a[0],
               last_modified_utc=ts_between(date(2023, 1, 1), date(2026, 10, 1)), group_type="Application") for a in APPS]
groups.append(dict(asset_group_id="GRP-021", asset_group_name="Data Platform (old)", application_asset_id="APP-015",
                   last_modified_utc="2023-02-14T09:12:00Z", group_type="Application"))
ak("cmdb", "M13", "ASSET_GROUPS", "GRP-021", "", "", "GRP-015", "Merge", "second group for the same application, no scopes")
with open(f"{OUT}/snowflake/cmdb/asset_groups.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(groups[0]))
    w.writeheader()
    w.writerows(groups)

# ======================================================================= actuals per cost center (clean truth, EffectiveCost)
app_month = defaultdict(float)
cc_month = defaultdict(float)
unalloc_month = defaultdict(float)
for p, d, row, r in truth_lines:
    if r["scope"]["app"]:
        app_month[(r["scope"]["app"], mkey(d))] += row["EffectiveCost"]
        cc_month[(APP[r["scope"]["app"]][3], mkey(d))] += row["EffectiveCost"]
    else:
        unalloc_month[mkey(d)] += row["EffectiveCost"]

# ======================================================================= financial budget (FY2026, per cost center and month)
BUD_COLS = ["enterprise_code", "enterprise_name", "business_unit_code", "business_unit_name", "business_code", "business_name",
            "department_code", "department_name", "cost_center_code", "cost_center_name", "budget_owner_id", "fiscal_year",
            "monthStart", "monthEnd", "budget_month", "budget_cost", "currency"]
OVERRUN = {"3170": 0.55, "2110": 0.80, "3590": 0.70, "0450": 0.85}   # FY2026 budget set too low -> overrun
budget = []
for cc, o in CC.items():
    for fy in FISCAL_YEARS:
        if fy == 2026:
            run_rate, factor = cc_month[(cc, "2026-07")], OVERRUN.get(cc, rnd.uniform(1.0, 1.2))
        else:
            run_rate = sum(cc_month[(cc, f"{fy}-{m:02d}")] for m in range(1, 13)) / 12
            factor = rnd.uniform(1.0, 1.15)
        base = max(500, round(run_rate * factor / 500) * 500)
        for m in range(1, 13):
            amt = base * (1.0 if m <= 6 else 1.03)
            budget.append(dict(enterprise_code="ZONTIQUE", enterprise_name="Zontique Inc.", business_unit_code=o[6],
                               business_unit_name=o[7], business_code=o[4], business_name=o[5], department_code=o[2],
                               department_name=o[3], cost_center_code=cc, cost_center_name=o[1], budget_owner_id=o[8],
                               fiscal_year=str(fy), monthStart="1", monthEnd="12", budget_month=f"{fy}-{m:02d}-01",
                               budget_cost=f"{round(amt, -1):.2f}", currency="USD"))
clean_budget = [dict(b) for b in budget]
for fy in FISCAL_YEARS:   # B07 cost center with budget but no application in the CMDB
    for m in range(1, 13):
        budget.append(dict(budget[0], business_unit_code="CORP", business_unit_name="Corporate Services", business_code="CORP-FUN",
                           business_name="Corporate Functions", department_code="DEP-STRAT", department_name="Corporate Strategy",
                           cost_center_code="3600", cost_center_name="Corporate Strategy", budget_owner_id="EMP-3008",
                           fiscal_year=str(fy), budget_month=f"{fy}-{m:02d}-01", budget_cost="4000.00"))
ak("financial", "B07", "BUDGET", "3600", "cost_center_code", "3600", "", "Flag", "budget for a cost center with no application or spend")
for b in budget:
    b["_key"] = f"{b['cost_center_code']}|{b['budget_month'][:7]}"
# B05 months missing
for mm in ("08", "09"):
    k = f"2150|2026-{mm}"
    budget = [b for b in budget if b["_key"] != k]
    ak("financial", "B05", "BUDGET", k, "", "", "", "Flag", "budget month missing")
# B02 leading zero lost on one cost center
for b in budget:
    if b["cost_center_code"] == "0420":
        b["cost_center_code"] = "420"
ak("financial", "B02", "BUDGET", "0420|*", "cost_center_code", "420", "0420", "Fix", "leading zero lost on all rows of the cost center")
cand = [b for b in budget if b["cost_center_code"] not in ("3600",)]
sel = rnd.sample(cand, 2 + 1 + 4 + 6)
for b in sel[:2]:   # B03 entered in thousands
    v = f"{float(b['budget_cost']) / 1000:.2f}"
    ak("financial", "B03", "BUDGET", b["_key"], "budget_cost", v, b["budget_cost"], "Flag", "amount entered in thousands")
    b["budget_cost"] = v
for b in sel[2:3]:   # B04 negative
    ak("financial", "B04", "BUDGET", b["_key"], "budget_cost", "-" + b["budget_cost"], b["budget_cost"], "Quarantine", "negative amount")
    b["budget_cost"] = "-" + b["budget_cost"]
for b in sel[3:7]:   # B06 month format
    d0 = date.fromisoformat(b["budget_month"])
    v = f"{d0:%m/%d/%Y}"
    ak("financial", "B06", "BUDGET", b["_key"], "budget_month", v, b["budget_month"], "Fix", "month not in yyyy-MM-dd")
    b["budget_month"] = v
for b in sel[7:]:   # B08 department name variant
    v = {"Data & Analytics": "Data and Analytics", "IT Operations": "IT Ops", "Legal & Compliance": "Legal and Compliance"}.get(
        b["department_name"], b["department_name"].upper())
    ak("financial", "B08", "BUDGET", b["_key"], "department_name", v, b["department_name"], "Fix", "department name variant")
    b["department_name"] = v
for b in rnd.sample(budget, 3):   # B01 duplicate line
    budget.append(dict(b))
    ak("financial", "B01", "BUDGET", b["_key"], "", "", "", "Merge", "line delivered twice")
rnd.shuffle(budget)
os.makedirs(f"{OUT}/snowflake/fin", exist_ok=True)
with open(f"{OUT}/snowflake/fin/financial_budget.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=BUD_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(budget)

# ======================================================================= VM Backups (Azure VMs only)
DRIFT_FROM = date(2026, 9, 16)
MISSING_DAYS = {date(2026, 7, 27), date(2026, 9, 21), date(2026, 10, 12)}
GB_PRICE = 0.05
vms = []
for r in az_vms:
    s = r["scope"]
    protected = s["env"] in ("prod", "shared") or rnd.random() < (0.5 if s["env"] == "test" else 0.3)
    vms.append(dict(name=r["name"], rid=r["id"], env=s["env"], region=s["region"], deleted=r["deleted"],
                    protected=protected, in_cmdb=s["in_cmdb"], base_gb=rnd.randint(40, 2000) if s["env"] == "prod" else rnd.randint(20, 300)))
for v in vms:
    if not v["in_cmdb"]:
        ak("vm-backups", "D02", "BACKUP_JOBS", v["name"], "", "", "", "Flag", "VM in a resource group the CMDB doesn't know")
prod_ok = [v for v in vms if v["env"] == "prod" and v["protected"] and not v["deleted"] and v["in_cmdb"]]
failing = set(v["name"] for v in rnd.sample(prod_ok, 4))
for n in sorted(failing):
    ak("vm-backups", "D03", "BACKUP_JOBS", n, "STATUS", "Failed", "Completed", "Flag",
       "no Completed job 2026-10-25..2026-10-31 (unprotected production VM)")
variant = {}
for v in rnd.sample(vms, round(len(vms) * 0.15)):
    variant[v["name"]] = v["name"].upper() if rnd.random() < 0.5 else v["name"] + ".zontique.local"
    ak("vm-backups", "D04", "BACKUP_JOBS", v["name"], "VM_NAME", variant[v["name"]], v["name"], "Fix", "VM name written differently in part of the files")


def shown(v):
    return variant[v["name"]] if v["name"] in variant and rnd.random() < 0.5 else v["name"]


def vault(v):
    return f"rsv-{'prod' if v['env'] in ('prod', 'shared') else 'nonprod'}-{v['region']}"


d09_days = set(rnd.sample([d for d in BK_DAYS if d not in MISSING_DAYS], 2))
jobs_by_day = {d: [] for d in BK_DAYS}
for v in vms:
    if not v["protected"]:
        continue
    daily = v["env"] in ("prod", "shared")
    for d in BK_DAYS:
        if v["deleted"] and d >= v["deleted"]:
            break
        if not daily and d.weekday() != 6:
            continue
        start = datetime(d.year, d.month, d.day, rnd.choice([0, 1, 2, 3, 22, 23]), rnd.randint(0, 59))
        status = rnd.choices(["Completed", "CompletedWithWarnings", "Failed"], [95, 3, 2])[0]
        if v["name"] in failing and d >= END - timedelta(6):
            status = "Failed"
        size = round(v["base_gb"] * rnd.uniform(0.01, 0.08), 2) if status != "Failed" else None
        job = dict(JOB_ID=uid(), VM=v, START=start, END=start + timedelta(minutes=rnd.randint(5, 120)), STATUS=status, SIZE=size,
                   ERROR=rnd.choice(["UserErrorVmNotRunning", "ExtensionSnapshotFailed", "BackUpOperationFailed"]) if status == "Failed" else "")
        if status == "Completed" and rnd.random() < 0.005:
            job["SIZE"] = rnd.choice([0, -round(rnd.uniform(1, 50), 2)])
            ak("vm-backups", "D05", "BACKUP_JOBS", job["JOB_ID"], "BACKUP_SIZE_GB", job["SIZE"], "", "Quarantine", "Completed job with size <= 0")
        jobs_by_day[d].append(job)
os.makedirs(f"{OUT}/vm_backups/jobs", exist_ok=True)
job_files, raw_rows, unique_jobs = {}, 0, set()
for d in BK_DAYS:
    if d in MISSING_DAYS:
        ak("vm-backups", "D08", "BACKUP_JOBS", f"backup_jobs_{d:%Y%m%d}.csv", "", "", "", "Flag", "daily file missing (completeness gap)")
        continue
    rows = list(jobs_by_day[d])
    prev = d - timedelta(1)
    overlap = [j for j in jobs_by_day.get(prev, []) if j["START"].hour >= 23]
    rows += overlap
    drift = d >= DRIFT_FROM
    ts = (lambda t: t.strftime("%d.%m.%Y %H:%M")) if d in d09_days else (lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ"))
    size_col = "BackupSizeGB" if drift else "BACKUP_SIZE_GB"
    cols = ["JOB_ID", "VM_NAME", "VM_RESOURCE_ID", "VAULT_NAME", "POLICY_NAME", "JOB_START_UTC", "JOB_END_UTC", "STATUS",
            size_col, "ERROR_CODE"] + (["SOURCE_REGION"] if drift else [])
    fn = f"backup_jobs_{d:%Y%m%d}.csv"
    with open(f"{OUT}/vm_backups/jobs/{fn}", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for j in rows:
            v = j["VM"]
            w.writerow([j["JOB_ID"], shown(v), v["rid"], vault(v), "Daily-30d" if v["env"] in ("prod", "shared") else "Weekly-12w",
                        ts(j["START"]), ts(j["END"]), j["STATUS"], "" if j["SIZE"] is None else j["SIZE"], j["ERROR"]]
                       + ([v["region"]] if drift else []))
            unique_jobs.add(j["JOB_ID"])
    if prev not in MISSING_DAYS:
        for j in overlap:
            ak("vm-backups", "D06", "BACKUP_JOBS", j["JOB_ID"], "", "", "", "Merge", f"repeated from backup_jobs_{prev:%Y%m%d}.csv")
    if d in d09_days:
        ak("vm-backups", "D09", "BACKUP_JOBS", fn, "JOB_START_UTC", "dd.MM.yyyy HH:mm", "yyyy-MM-ddTHH:mm:ssZ", "Fix", "timestamp format")
    job_files[fn] = len(rows)
    raw_rows += len(rows)
ak("vm-backups", "D07", "BACKUP_JOBS", f"backup_jobs_{DRIFT_FROM:%Y%m%d}.csv", "BACKUP_SIZE_GB", "BackupSizeGB", "BACKUP_SIZE_GB", "Fix",
   "schema drift: column renamed, new column SOURCE_REGION from this day on")
os.makedirs(f"{OUT}/vm_backups/storage", exist_ok=True)
storage_totals, orphan_totals, orphan_vms, backup_cost_vm = {}, {}, set(), defaultdict(float)
for ms in BK_MONTHS:
    month, fn = mkey(ms), f"backup_storage_{ms:%Y%m}.csv"
    total = orphan = 0.0
    with open(f"{OUT}/vm_backups/storage/{fn}", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["MONTH", "VM_NAME", "VM_RESOURCE_ID", "VAULT_NAME", "PROTECTION_STATE", "RETAINED_STORAGE_GB", "STORAGE_COST_USD"])
        for v in vms:
            if not v["protected"]:
                continue
            stopped = v["deleted"] is not None and v["deleted"] <= ms
            gb = v["base_gb"] if not stopped else round(v["base_gb"] * 0.9, 0)
            cost = round(gb * GB_PRICE, 2)
            w.writerow([month, shown(v), v["rid"], vault(v), "ProtectionStopped" if stopped else "Protected", gb, f"{cost:.2f}"])
            total += cost
            backup_cost_vm[(v["name"], month)] += cost
            if stopped:
                orphan += cost
                orphan_vms.add(v["name"])
                ak("vm-backups", "D01", "BACKUP_STORAGE", f"{v['name']}|{month}", "PROTECTION_STATE", "ProtectionStopped", "", "Flag",
                   f"VM deleted {v['deleted']} (no cost lines after that day); backup storage still billed {cost:.2f} USD")
    storage_totals[month], orphan_totals[month] = round(total, 2), round(orphan, 2)

# ======================================================================= License & Inventory (Azure SQL)
PRODUCTS = [(1, "Windows Server 2019 Standard", "Windows Server", "Standard"), (2, "Windows Server 2019 Datacenter", "Windows Server", "Datacenter"),
            (3, "Windows Server 2022 Standard", "Windows Server", "Standard"), (4, "Windows Server 2022 Datacenter", "Windows Server", "Datacenter"),
            (5, "SQL Server 2019 Standard", "SQL Server", "Standard"), (6, "SQL Server 2019 Enterprise", "SQL Server", "Enterprise"),
            (7, "SQL Server 2022 Standard", "SQL Server", "Standard"), (8, "SQL Server 2022 Enterprise", "SQL Server", "Enterprise")]
VARIANTS = {1: ["Win Srv 2019 Std", "Microsoft Windows Server 2019 Standard Edition"], 2: ["Win2019 DC", "Windows Server 2019 DataCenter"],
            3: ["Win Srv 2022 Std", "WS2022 Standard"], 4: ["Win Srv 2022 DC", "Windows Server 2022 Datacenter Edition"],
            5: ["MS SQL Svr 2019 Std", "SQL2019 STD"], 6: ["MS SQL Server 2019 Ent", "SQL 2019 EE"],
            7: ["MS SQL Svr 2022 Std", "SQL2022 Standard Ed."], 8: ["SQL Server 2022 Ent.", "MSSQL 2022 EE"]}
installs = []
for r in az_vms:
    s = r["scope"]
    win = rnd.choice([3, 3, 4, 1])
    rows = [win]
    if s["tier"] == "data" or (s["app"] and APP[s["app"]][2] in ("EDPLAT", "BILL", "GLEDG", "DMDPLN")):
        rows.append(rnd.choice([7, 7, 8, 5]))
    if rnd.random() < 0.3:
        rows.append(win - 2 if win > 2 else win + 2)
    for pid in rows:
        model = "PAYG" if (r["payg"] and pid <= 4) else "BYOL"
        last_seen = (r["deleted"] - timedelta(1)) if r["deleted"] else END - timedelta(rnd.randint(0, 5))
        installs.append(dict(host_name=r["name"], raw_product_name=PRODUCTS[pid - 1][1], product_id=pid, core_count=r["cores"],
                             license_model=model, environment=s["env"], install_date=str(rdate(date(2023, 1, 1), date(2026, 6, 1))),
                             last_seen_date=str(last_seen), _vm=r, _pid=pid))
rnd.shuffle(installs)
for i, x in enumerate(installs, 1):
    x["installation_id"] = 100000 + i
entitled_need = defaultdict(int)
for x in installs:
    if x["license_model"] == "BYOL":
        entitled_need[x["product_id"]] += x["core_count"]
ents, eid = [], 5000
over = {3: 0.75, 7: 0.6}   # D07 over-deployment on two products
for pid, need in sorted(entitled_need.items()):
    cover = over.get(pid, rnd.uniform(1.05, 1.3))
    qty = int(round(need * cover / 2) * 2)
    parts = [qty] if qty < 64 else [qty // 2, qty - qty // 2]
    for q in parts:
        eid += 1
        ents.append(dict(entitlement_id=eid, product_id=pid, agreement_number=f"EA-{rnd.randint(1000000, 9999999)}", quantity=q,
                         start_date="2024-07-01", end_date="2027-06-30",
                         unit_cost_usd={1: 120, 2: 680, 3: 130, 4: 720, 5: 1900, 6: 7100, 7: 2000, 8: 7400}[pid], license_model="BYOL"))
for pid in sorted(over):
    ak("license", "D07", "license_entitlements", f"product {pid}", "quantity", sum(e["quantity"] for e in ents if e["product_id"] == pid),
       entitled_need[pid], "Flag", "BYOL cores installed exceed entitled cores (over-deployment)")
for e in rnd.sample([e for e in ents if e["product_id"] not in over], 2):   # D08 expired
    e["end_date"] = "2026-06-30"
    ak("license", "D08", "license_entitlements", e["entitlement_id"], "end_date", "2026-06-30", "", "Flag",
       "entitlement expired before 2026-10-31 but still the only coverage")
# defects on installations (at most one per row)
pool_i = list(installs)
rnd.shuffle(pool_i)
n = len(installs)
for x in pool_i[:round(n * 0.30)]:   # D01
    v = rnd.choice(VARIANTS[x["product_id"]])
    ak("license", "D01", "installations", x["installation_id"], "raw_product_name", v, PRODUCTS[x["product_id"] - 1][1], "AI-classify",
       "discovery name variant, product_id empty")
    x["raw_product_name"], x["product_id"] = v, ""
rest = pool_i[round(n * 0.30):]
for x in rest[:round(n * 0.03)]:   # D05
    ak("license", "D05", "installations", x["installation_id"], "core_count", "", x["core_count"], "Fix", "core count empty: take it from the VM size")
    x["core_count"] = ""
rest = rest[round(n * 0.03):]
payg_byol = [x for x in rest if x["_vm"]["payg"] and x["product_id"] and x["product_id"] <= 4][:4]
for x in payg_byol:   # D06 double licensing
    x["license_model"] = "BYOL"
    ak("license", "D06", "installations", x["installation_id"], "license_model", "BYOL", "PAYG", "Flag",
       f"{x['host_name']} has pay-as-you-go Windows charges in FOCUS: paying twice")
for x in installs:   # D03 deleted VM still seen
    if x["_vm"]["deleted"]:
        x["last_seen_date"] = str(END - timedelta(rnd.randint(0, 3)))
        ak("license", "D03", "installations", x["installation_id"], "last_seen_date", x["last_seen_date"], "", "Flag",
           f"VM deleted {x['_vm']['deleted']} but still reported as installed")
ghosts = []
for i in range(round(n * 0.03)):   # D02 host not in CMDB / FOCUS
    h = f"srv-{rnd.choice(['fin', 'hr', 'ops', 'lab'])}-{rnd.randint(10, 99)}"
    g = dict(installation_id=100000 + n + i + 1, host_name=h, raw_product_name=PRODUCTS[2][1], product_id=3, core_count=rnd.choice([4, 8]),
             license_model="BYOL", environment="prod", install_date="2021-03-15", last_seen_date=str(END - timedelta(rnd.randint(0, 6))))
    ghosts.append(g)
    ak("license", "D02", "installations", g["installation_id"], "host_name", h, "", "Flag", "host not in the CMDB or FOCUS (unmanaged)")
dups = []
for x in rnd.sample([x for x in installs if x not in payg_byol], round(n * 0.02)):   # D04
    d2 = dict(x, installation_id=100000 + n + len(ghosts) + len(dups) + 1)
    dups.append(d2)
    ak("license", "D04", "installations", d2["installation_id"], "", "", x["installation_id"], "Merge", "duplicate installation row")
installs_out = sorted(installs + ghosts + dups, key=lambda x: x["installation_id"])
os.makedirs(f"{OUT}/azure_sql", exist_ok=True)
INS_COLS = ["installation_id", "host_name", "raw_product_name", "product_id", "core_count", "license_model", "environment",
            "install_date", "last_seen_date"]
tables = {"license_products": (["product_id", "product_name", "product_family", "edition", "license_metric"],
                               [dict(product_id=p[0], product_name=p[1], product_family=p[2], edition=p[3], license_metric="Per Core") for p in PRODUCTS]),
          "license_entitlements": (list(ents[0]), ents), "installations": (INS_COLS, installs_out)}
for t, (cols, rows) in tables.items():
    with open(f"{OUT}/azure_sql/{t}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def sqlv(v):
    if v == "" or v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "N'" + str(v).replace("'", "''") + "'"


DDL = {"license_products": "product_id int NOT NULL PRIMARY KEY, product_name nvarchar(100) NOT NULL, product_family nvarchar(50), edition nvarchar(50), license_metric nvarchar(30)",
       "license_entitlements": "entitlement_id int NOT NULL PRIMARY KEY, product_id int NOT NULL, agreement_number nvarchar(20), quantity int, start_date date, end_date date, unit_cost_usd decimal(18,2), license_model nvarchar(10)",
       "installations": "installation_id bigint NOT NULL PRIMARY KEY, host_name nvarchar(100), raw_product_name nvarchar(150), product_id int NULL, core_count int NULL, license_model nvarchar(10), environment nvarchar(20), install_date date, last_seen_date date"}
with open(f"{OUT}/azure_sql/license_load.sql", "w", encoding="utf-8") as f:
    f.write("-- Legacy Defuse Kit | License & Inventory (Azure SQL, schema dbo). Generated, seed %d.\n" % SEED)
    f.write("-- Recreates the 3 tables. If Fabric mirroring is running on them: stop it first, start it again after.\n")
    f.write("-- No foreign keys on purpose (legacy system; installations.product_id can be empty).\nSET NOCOUNT ON;\n")
    for t in ("installations", "license_entitlements", "license_products"):
        f.write(f"DROP TABLE IF EXISTS dbo.{t};\n")
    f.write("GO\n")
    for t, (cols, rows) in tables.items():
        f.write(f"CREATE TABLE dbo.{t} ({DDL[t]});\nGO\n")
        for i in range(0, len(rows), 500):
            f.write(f"INSERT INTO dbo.{t} ({', '.join(cols)}) VALUES\n")
            f.write(",\n".join("(" + ", ".join(sqlv(r[c]) for c in cols) + ")" for r in rows[i:i + 500]) + ";\nGO\n")
    f.write("SELECT 'license_products' t, COUNT(*) n FROM dbo.license_products UNION ALL SELECT 'license_entitlements', COUNT(*) FROM dbo.license_entitlements UNION ALL SELECT 'installations', COUNT(*) FROM dbo.installations;\n")

# ======================================================================= expected totals
os.makedirs(f"{OUT}/answer-keys", exist_ok=True)


def write_key(system, expected):
    os.makedirs(f"{OUT}/answer-keys/{system}", exist_ok=True)
    with open(f"{OUT}/answer-keys/{system}/answer_key.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=AK_COLS)
        w.writeheader()
        w.writerows(AK[system])
    counts = defaultdict(int)
    for a in AK[system]:
        counts[a["defect_id"]] += 1
    expected["defect_counts"] = dict(sorted(counts.items()))
    with open(f"{OUT}/answer-keys/{system}/expected_totals.json", "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=2, default=str)


def fsum(rows, col):
    return round(sum(float(r[col] or 0) for r in rows), 2)


all_cost = [r for rows in files.values() for r in rows]
dup_rows = files["focus_azure_202608_rerun_20260815.csv"]
by_pm = defaultdict(lambda: [0, 0.0, 0.0])
for p, d, row, r in truth_lines + extra_lines:
    if p == "GCP" and d == GAP_DAY:
        continue
    k = f"{p}|{mkey(d)}"
    by_pm[k][0] += 1
    by_pm[k][1] += row["BilledCost"]
    by_pm[k][2] += row["EffectiveCost"]
write_key("cost", {
    "seed": SEED, "period": [START, END], "focus_version": "1.2", "files": {k: len(v) for k, v in sorted(files.items())},
    "rows_delivered": len(all_cost), "rows_after_dedupe": len(all_cost) - len(dup_rows),
    "billed_cost_delivered": fsum(all_cost, "BilledCost"), "billed_cost_after_dedupe": round(fsum(all_cost, "BilledCost") - fsum(dup_rows, "BilledCost"), 2),
    "effective_cost_delivered": fsum(all_cost, "EffectiveCost"),
    "effective_cost_after_dedupe": round(fsum(all_cost, "EffectiveCost") - fsum(dup_rows, "EffectiveCost"), 2),
    "per_provider_month_after_dedupe": {k: dict(rows=v[0], billed=round(v[1], 2), effective=round(v[2], 2)) for k, v in sorted(by_pm.items())},
    "gcp_gap_day": GAP_DAY, "gcp_gap_estimated_billed": round(gap_lines[1], 2),
    "effective_cost_by_app_month": {f"{k[0]}|{k[1]}": round(v, 2) for k, v in sorted(app_month.items())},
    "unallocated_effective_cost_by_month": {k: round(v, 2) for k, v in sorted(unalloc_month.items())},
    "zombie_scopes": [s["name"] for s in ZOMBIE_SCOPES], "shadow_scopes": [s["name"] for s in SHADOW],
    "note": "Use EffectiveCost for chargeback and budgets, BilledCost to reconcile with the invoice. Reservation purchases: BilledCost only; covered usage: EffectiveCost only.",
})
write_key("cmdb", {"seed": SEED, "assets_rows_delivered": len(all_assets), "applications_true": len(APPS), "scopes_true": len(cmdb_scopes),
                   "asset_groups_delivered": len(groups), "asset_groups_true": len(APPS),
                   "scope_owner_true": {s["name"]: APP[s["app"]][2] for s in cmdb_scopes}})
write_key("financial", {"seed": SEED, "rows_delivered": len(budget), "rows_true": len(clean_budget) + 12 * len(FISCAL_YEARS),
                  "budget_true_by_fy_cc": {f"{fy}|{cc}": round(sum(float(b["budget_cost"]) for b in clean_budget
                                                                    if b["cost_center_code"] == cc and b["fiscal_year"] == str(fy)), 2)
                                           for fy in FISCAL_YEARS for cc in CC},
                  "budget_true_total": round(sum(float(b["budget_cost"]) for b in clean_budget), 2),
                  "overrun_cost_centers": sorted(OVERRUN)})
write_key("vm-backups", {"seed": SEED, "period": [RECENT, END], "vms_total": len(vms), "vms_protected": sum(v["protected"] for v in vms),
                  "vms_not_in_cmdb": [v["name"] for v in vms if not v["in_cmdb"]], "job_files": len(job_files), "job_rows_raw": raw_rows,
                  "job_ids_unique": len(unique_jobs), "missing_job_files": sorted(f"backup_jobs_{d:%Y%m%d}.csv" for d in MISSING_DAYS),
                  "storage_rows_per_month": sum(v["protected"] for v in vms), "storage_cost_usd": storage_totals,
                  "orphaned_backup_cost_usd": orphan_totals, "orphaned_vms": sorted(orphan_vms), "unprotected_prod_vms": sorted(failing)})
true_byol = defaultdict(int)
for x in installs:
    pid = x["_pid"]
    if not (x["_vm"]["payg"] and pid <= 4):
        true_byol[pid] += x["_vm"]["cores"]
write_key("license", {"seed": SEED, "rows_delivered": {t: len(v[1]) for t, v in tables.items()},
                  "installations_true": len(installs), "byol_cores_installed_true": dict(sorted(true_byol.items())),
                  "cores_entitled": {pid: sum(e["quantity"] for e in ents if e["product_id"] == pid) for pid in sorted(entitled_need)},
                  "double_licensed_vms": sorted(x["host_name"] for x in payg_byol),
                  "annual_entitlement_cost_usd": round(sum(e["quantity"] * e["unit_cost_usd"] for e in ents), 2)})
gold = {}
for cc in CC:
    for ms in MONTHS:
        m = mkey(ms)
        bud = next(float(b["budget_cost"]) for b in clean_budget if b["cost_center_code"] == cc and b["budget_month"].startswith(m))
        act = round(cc_month[(cc, m)], 2)
        gold[f"{cc}|{m}"] = dict(budget=bud, actual_effective=act, variance=round(act - bud, 2))
with open(f"{OUT}/answer-keys/expected_gold.json", "w", encoding="utf-8") as f:
    json.dump({"budget_vs_actual_by_cc_month": gold, "unallocated_by_month": {k: round(v, 2) for k, v in sorted(unalloc_month.items())},
               "backup_cost_by_vm_month": {f"{k[0]}|{k[1]}": round(v, 2) for k, v in sorted(backup_cost_vm.items())}}, f, indent=2)
print(json.dumps({"cost_rows": len(all_cost), "cost_files": len(files), "assets": len(all_assets), "budget_rows": len(budget),
                  "vms_azure": len(az_vms), "backup_job_rows": raw_rows, "installations": len(installs_out),
                  "defects": {s: len(v) for s, v in AK.items()}}, indent=1))
