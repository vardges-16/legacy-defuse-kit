# Answer keys: ground truth for scoring only

Never an input to cleansing. Only `nb_reconcile` and `nb_ai_score` read them, from `lh_trust` Files `answer_keys/` (uploaded once). Silver notebooks must not reference them (governance policy check).

Generated with the data by `python generators/generate_zontique.py data/generated` (seed 42); this folder is the committed copy of `data/generated/answer-keys/`. Recopy only when you deliberately regenerate.

| File | Content |
|---|---|
| `<system>/answer_key.csv` | One row per planted defect: `defect_id, table_name, record_id, field, delivered_value, expected_value, expected_action, detail` |
| `<system>/expected_totals.json` | Row counts and amounts as delivered and as they should be after cleansing |
| `expected_gold.json` | Budget vs actual (EffectiveCost) per cost center and month, unallocated cost, backup cost per VM |
