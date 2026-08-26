# GP rostering — ED / obstetrics separation (JT & CB consolidation trial)

Constraint-based roster generator and briefing demo for the proposed 6-month
trial of a single alternating maternity team across Jamestown and Crystal
Brook, with the ED roster run separately.

## What's here

| File | Purpose |
|---|---|
| `workforce_config.json` | **Edit this first.** Doctors, skills (ED/GPO/GPA/OBS), home site, FTE, reduced-load flags, incoming GPOs (Lauren/Krishna), Clare backup pool, fatigue rules. |
| `scenarios.json` | The months to solve: model (old two-team vs new single-team), active birthing site, leave requests. |
| `roster_solver.py` | The generator (Google OR-Tools CP-SAT). Solves every scenario, prints a summary, writes `results.json`. |
| `results.json` | Generated rosters + per-doctor statistics + coverage summaries. |
| `build_dashboard.py` | Injects `results.json` into `dashboard_template.html` → `dashboard.html` (the briefing page). |

## Run it

```bash
pip install ortools
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

## Swapping in the real workforce

The doctors in `workforce_config.json` are a realistic stand-in (the real
July/August rosters weren't available when this was built). To make the
numbers quotable: replace the `doctors` array with the real roster's names,
skills and FTEs, copy real leave into `scenarios.json`, and re-run the two
commands above. The dashboard regenerates itself from the results.
