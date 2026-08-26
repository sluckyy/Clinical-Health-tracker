"""Constraint-based on-call roster generator for the JT/CB GP workforce.

Models two rostering worlds:

  OLD model - two full maternity teams (GPO+GPA+OBS at BOTH Jamestown and
  Crystal Brook every day = 6 procedural roles) plus 2 ED lines. Because the
  workforce cannot fill 8 roles/day, doctors are allowed to "double-hat"
  (hold ED and a same-site procedural role simultaneously) - the historic
  practice that produced the Orroroo cord-prolapse near miss.

  NEW model - ONE maternity team (GPO+GPA+OBS, active birthing site
  alternating between JT and CB) plus 2 ED lines = 5 roles/day, with a hard
  rule that nobody ever covers two things at once.

Solved with Google OR-Tools CP-SAT. Clare network backup is available for
maternity roles (GPO/GPA/OBS) at a penalty; an unfilled ED day is a "gap"
at a very large penalty, so the solver only ever reports an ED gap when the
day is genuinely uncoverable.
"""

import json
from datetime import date, timedelta
from pathlib import Path

from ortools.sat.python import cp_model

HERE = Path(__file__).parent

ED_ROLES_BY_SITE = {"ED_JT": "JT", "ED_CB": "CB"}
NEW_MODEL_ROLES = ["ED_JT", "ED_CB", "GPO", "GPA", "OBS"]
OLD_MODEL_ROLES = ["ED_JT", "ED_CB",
                   "GPO_JT", "GPO_CB", "GPA_JT", "GPA_CB", "OBS_JT", "OBS_CB"]

GAP_COST = 10000          # an actually-unfilled role-day
CLARE_COST = 300          # Clare backup covering a maternity role-day
REDUCED_LOAD_COST = 60    # each on-call day carried by a reduced-load doctor
OVER_TARGET_COST = 120    # each day above a doctor's fair-share target
CONTINUITY_COST = 25      # maternity role changing hands between days
HOME_MISMATCH_COST = 4    # ED line covered by a doctor from the other site


def role_skill(role):
    """Skill needed for a role (strips the _JT/_CB site suffix)."""
    return role.split("_")[0] if not role.startswith("ED") else "ED"


def role_site(role, active_site):
    if role in ED_ROLES_BY_SITE:
        return ED_ROLES_BY_SITE[role]
    if role.endswith("_JT"):
        return "JT"
    if role.endswith("_CB"):
        return "CB"
    return active_site  # single-team maternity roles sit at the active site


def build_and_solve(scenario, config):
    year, month = scenario["year"], scenario["month"]
    first = date(year, month, 1)
    ndays = ((date(year + month // 12, month % 12 + 1, 1)) - first).days
    days = [first + timedelta(d) for d in range(ndays)]

    model_kind = scenario["model"]  # "old" | "new"
    roles = OLD_MODEL_ROLES if model_kind == "old" else NEW_MODEL_ROLES
    maternity_roles = [r for r in roles if not r.startswith("ED")]
    active_site = scenario.get("active_site", "JT")

    doctors = [dict(d) for d in config["doctors"]]
    for extra_id in scenario.get("extra_doctors", []):
        fd = next(f for f in config["future_doctors"] if f["id"] == extra_id)
        doctors.append(dict(fd))

    if model_kind == "old":
        # the old roster could not accommodate anyone stepping back - with 8
        # roles/day everyone shares the crush, so reduced-load preferences
        # are ignored in the baseline
        for doc in doctors:
            doc["reduced_load"] = False

    leave = {doc["id"]: set() for doc in doctors}
    for entry in scenario.get("leave", []):
        for d in range(entry["from_day"], entry["to_day"] + 1):
            if entry["doc"] in leave:
                leave[entry["doc"]].add(d)

    rules = config["rules"]
    max_consec = rules["max_consecutive_oncall_days"]

    m = cp_model.CpModel()
    x = {}      # (doc_id, day_index, role) -> BoolVar
    clare = {}  # (day_index, role) -> BoolVar, maternity roles only
    gap = {}    # (day_index, role) -> BoolVar

    for t in range(ndays):
        for role in roles:
            for doc in doctors:
                if role_skill(role) in doc["skills"] and (t + 1) not in leave[doc["id"]]:
                    x[doc["id"], t, role] = m.NewBoolVar(f"x_{doc['id']}_{t}_{role}")
            if role in maternity_roles:
                clare[t, role] = m.NewBoolVar(f"clare_{t}_{role}")
            gap[t, role] = m.NewBoolVar(f"gap_{t}_{role}")

    def cover(t, role):
        docs_avail = [x[d["id"], t, role] for d in doctors if (d["id"], t, role) in x]
        extras = [clare[t, role]] if (t, role) in clare else []
        return docs_avail + extras + [gap[t, role]]

    # every role covered by exactly one person (or Clare, or flagged as a gap)
    for t in range(ndays):
        for role in roles:
            m.AddExactlyOne(cover(t, role))

    # roles per doctor per day
    on = {}  # (doc_id, t) -> BoolVar: doctor is on call at all that day
    for doc in doctors:
        for t in range(ndays):
            todays = [x[doc["id"], t, r] for r in roles if (doc["id"], t, r) in x]
            o = m.NewBoolVar(f"on_{doc['id']}_{t}")
            on[doc["id"], t] = o
            if not todays:
                m.Add(o == 0)
                continue
            m.AddMaxEquality(o, todays)
            if model_kind == "new":
                # THE core rule of the new model: never two hats on one head
                m.AddAtMostOne(todays)
            else:
                # old world: ED + one same-site procedural role could be
                # (and was) held simultaneously
                m.Add(sum(todays) <= 2)
                for r1 in roles:
                    for r2 in roles:
                        if r1 < r2 and (doc["id"], t, r1) in x and (doc["id"], t, r2) in x:
                            same_site = role_site(r1, active_site) == role_site(r2, active_site)
                            ed_pair = r1.startswith("ED") != r2.startswith("ED")
                            if not (same_site and ed_pair):
                                m.AddAtMostOne([x[doc["id"], t, r1], x[doc["id"], t, r2]])

    # fatigue rule: no more than max_consec consecutive on-call days
    for doc in doctors:
        for t in range(ndays - max_consec):
            m.Add(sum(on[doc["id"], t + k] for k in range(max_consec + 1)) <= max_consec)

    # ---- objective ----
    terms = []
    for t in range(ndays):
        for role in roles:
            terms.append(gap[t, role] * GAP_COST)
            if (t, role) in clare:
                terms.append(clare[t, role] * CLARE_COST)

    n_slots = len(roles) * ndays
    full_fte = sum(d.get("fte", 1.0) for d in doctors)
    for doc in doctors:
        total = sum(on[doc["id"], t] for t in range(ndays))
        share = n_slots * doc.get("fte", 1.0) / full_fte
        if doc.get("reduced_load"):
            target = int(share * rules["reduced_load_target_fraction"])
            for t in range(ndays):
                terms.append(on[doc["id"], t] * REDUCED_LOAD_COST)
        else:
            target = int(share) + 1
        over = m.NewIntVar(0, ndays, f"over_{doc['id']}")
        m.Add(over >= total - target)
        terms.append(over * OVER_TARGET_COST)

    # maternity continuity: prefer the same doctor holding a maternity role
    # on consecutive days (week-style blocks, fewer handovers)
    for role in maternity_roles:
        for doc in doctors:
            for t in range(ndays - 1):
                if (doc["id"], t, role) in x and (doc["id"], t + 1, role) in x:
                    switch = m.NewBoolVar(f"sw_{doc['id']}_{t}_{role}")
                    m.Add(x[doc["id"], t, role] - x[doc["id"], t + 1, role] <= switch)
                    terms.append(switch * CONTINUITY_COST)

    # soft preference: ED line covered from its own town (Orroroo/Laura links)
    for t in range(ndays):
        for role, site in ED_ROLES_BY_SITE.items():
            for doc in doctors:
                if (doc["id"], t, role) in x and doc["home"] != site:
                    terms.append(x[doc["id"], t, role] * HOME_MISMATCH_COST)

    m.Minimize(sum(terms))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60
    solver.parameters.num_workers = 8
    status = solver.Solve(m)
    assert status in (cp_model.OPTIMAL, cp_model.FEASIBLE), "solver failed"

    # ---- extract ----
    roster = []
    for t, day in enumerate(days):
        row = {"date": day.isoformat(), "weekday": day.strftime("%a")}
        for role in roles:
            who = None
            for doc in doctors:
                if (doc["id"], t, role) in x and solver.Value(x[doc["id"], t, role]):
                    who = doc["id"]
            if who is None and (t, role) in clare and solver.Value(clare[t, role]):
                who = "CLARE"
            if who is None and solver.Value(gap[t, role]):
                who = "GAP"
            row[role] = who
        roster.append(row)

    stats = {}
    for doc in doctors:
        ed_days = mat_days = wk_days = double = 0
        streak = best_streak = 0
        for t, day in enumerate(days):
            roles_today = [r for r in roles if (doc["id"], t, r) in x and solver.Value(x[doc["id"], t, r])]
            if roles_today:
                if any(r.startswith("ED") for r in roles_today):
                    ed_days += 1
                if any(not r.startswith("ED") for r in roles_today):
                    mat_days += 1
                if len(roles_today) > 1:
                    double += 1
                if day.weekday() >= 5:
                    wk_days += 1
                streak += 1
                best_streak = max(best_streak, streak)
            else:
                streak = 0
        total_on = sum(solver.Value(on[doc["id"], t]) for t in range(ndays))
        stats[doc["id"]] = {
            "name": doc["name"], "total_oncall_days": total_on,
            "ed_days": ed_days, "maternity_days": mat_days,
            "weekend_days": wk_days, "double_hatted_days": double,
            "longest_stretch": best_streak,
            "leave_days": len(leave[doc["id"]]),
            "reduced_load": bool(doc.get("reduced_load")),
        }

    n_gaps = sum(solver.Value(g) for g in gap.values())
    ed_gaps = sum(solver.Value(gap[t, r]) for t in range(ndays)
                  for r in roles if r.startswith("ED"))
    clare_days = sum(solver.Value(c) for c in clare.values())
    ed_slots = 2 * ndays

    return {
        "scenario": scenario["id"],
        "label": scenario["label"],
        "model": model_kind,
        "month": f"{year}-{month:02d}",
        "active_site": active_site,
        "days": ndays,
        "roles": roles,
        "roster": roster,
        "doctor_stats": stats,
        "summary": {
            "ed_slots": ed_slots,
            "ed_gaps": ed_gaps,
            "ed_fill_pct": round(100 * (ed_slots - ed_gaps) / ed_slots, 1),
            "total_gaps": n_gaps,
            "clare_backup_days": clare_days,
            "double_hatted_days": sum(s["double_hatted_days"] for s in stats.values()),
        },
    }


def main():
    config = json.loads((HERE / "workforce_config.json").read_text())
    scenarios = json.loads((HERE / "scenarios.json").read_text())
    results = [build_and_solve(s, config) for s in scenarios]
    (HERE / "results.json").write_text(json.dumps(results, indent=1))

    for r in results:
        s = r["summary"]
        print(f"\n=== {r['label']} ({r['month']}, {r['model'].upper()} model) ===")
        print(f"  ED fill: {s['ed_fill_pct']}%  (gaps: {s['ed_gaps']}/{s['ed_slots']})"
              f"  | total unfilled role-days: {s['total_gaps']}"
              f"  | Clare backup days: {s['clare_backup_days']}"
              f"  | double-hatted days: {s['double_hatted_days']}")
        for did, st in sorted(r["doctor_stats"].items(),
                              key=lambda kv: -kv[1]["total_oncall_days"]):
            flag = " (reduced-load)" if st["reduced_load"] else ""
            print(f"  {st['name']:<22} on-call {st['total_oncall_days']:>2}d "
                  f"(ED {st['ed_days']}, maternity {st['maternity_days']}, "
                  f"wkend {st['weekend_days']}, dbl {st['double_hatted_days']}, "
                  f"max stretch {st['longest_stretch']}){flag}")


if __name__ == "__main__":
    main()
