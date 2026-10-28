# Legacy Defuse Kit

**AI-Accelerated Legacy-to-Fabric Modernization Factory** — hackathon entry of team **de_dust2** (2 data engineers, 1 BI analyst) for the Advanced, cross-industry challenge.

Legacy Defuse Kit moves Zontique Corp's cost data out of five legacy systems into a governed Microsoft Fabric Lakehouse, cleans it with AI, delivers it through a shared Power BI report, and proves with evidence that nothing was lost on the way.

> **Status:** blueprint and architecture phase. This repository currently holds the design documents only; the solution code is not part of it yet.

## Documents

| Document | Purpose |
|---|---|
| [`docs/blueprint.md`](docs/blueprint.md) | Hackathon blueprint: team and challenge statement, Microsoft technology touched, business impact, technical architecture, delivery plan |
| [`docs/sad.md`](docs/sad.md) | Software Architecture Document (SAD): scope, goals, system context, architecture overview, runtime flows |
| [`docs/submission-form-fields.md`](docs/submission-form-fields.md) | Text for the submission form fields (idea name, target industry, business usecase, business impact, architecture) |
| [`docs/diagrams/`](docs/diagrams/) | Six architecture diagrams (PNG) used by the blueprint and the SAD |
| [`docs/pdf/`](docs/pdf/) | PDF versions of the blueprint and the SAD |

## Repository structure

```
legacy-defuse-kit/
├── README.md
├── .gitlab-ci.yml                  (existing, SAST)
└── docs/
    ├── blueprint.md
    ├── sad.md
    ├── submission-form-fields.md
    ├── diagrams/
    │   ├── 01_solution_overview.png
    │   ├── 02_technical_architecture.png
    │   ├── 03_deployment_view.png
    │   ├── 04_user_workflow.png
    │   ├── 05_security_compliance.png
    │   └── 06_gold_data_model.png
    └── pdf/
        ├── Legacy_Defuse_Kit_Blueprint.pdf
        └── Legacy_Defuse_Kit_SAD.pdf
```

## Team

de_dust2
