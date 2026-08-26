# GP rostering — ED / obstetrics separation (JT & CB consolidation trial)

Constraint-based roster generator and briefing demo for the proposed 6-month
trial of a single alternating maternity team across Jamestown and Crystal
Brook, with the ED roster run separately. The baseline is the **actual
July–August 2026 roster**, parsed from the spreadsheet in `data/`.

## What's here

| File | Purpose |
|---|---|
| `data/GP_roster_jul_aug_2026.xlsx` | The real roster (old model) as provided. |
| `extract_actuals.py` | Parses the spreadsheet → `actuals.json`: per-day assignments, multi-hatting, gaps, DIVERT days, per-doctor stats. Normalises name typos and the "(SJ)/(HD)" backup annotations. |
| `workforce_config.json` | **Edit this to update the workforce.** Real doctors with skills inferred from the lines they actually work, FTE estimates, reduced-load flags, Clare backup pool, fatigue rules. |
| `scenarios.json` | The months to generate: active birthing site, leave requests, skill upgrades (e.g. Lauren McLean → GPO). |
| `roster_solver.py` | The generator (Google OR-Tools CP-SAT). Solves every scenario, prints a summary, writes `results.json`. |
| `build_dashboard.py` | Injects `actuals.json` + `results.json` into `dashboard_template.html` → `dashboard.html` (the briefing page). |

## Run it

```bash
pip install ortools openpyxl
python3 extract_actuals.py data/GP_roster_jul_aug_2026.xlsx
python3 roster_solver.py      # solves all scenarios (~1 min)
python3 build_dashboard.py    # rebuilds the briefing dashboard
```

## The model in one paragraph

Every day needs a set of on-call roles covered. Old model: 2 ED lines + a full
GPO/GPA/obs-support team at **both** sites = 8 roles/day, with doctors allowed
to hold ED and a same-site obstetric role simultaneously ("double-hatting" —
the historic practice). New model: 2 ED lines + **one** maternity team = 5
roles/day, with a hard rule that nobody covers two things at once. Hard
constraints: skill match, leave, max 4 consecutive on-call days, every role
covered (uncoverable days are flagged, never hidden). Soft objectives:
reduced-load doctors steered to ~55% of a fair share, FTE-scaled fairness,
week-style maternity blocks, home-site ED preference, Clare-network backup
(GPO/GPA/OBS only) used last and always counted.

## Known assumptions to review

- FTE values are inferred from appearance frequency on the actual roster,
  not payroll — adjust in `workforce_config.json`.
- Future-month leave in `scenarios.json` is assumed for the demo; swap in
  real requests before circulating per-doctor numbers.
- Laura and Orroroo are folded into the CB and JT ED lines respectively
  (Laura's separate line was already unrostered in August). To keep them as
  separate lines, add the roles in `roster_solver.py`'s `NEW_MODEL_ROLES`.
