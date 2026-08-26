"""Inject results.json into dashboard_template.html -> dashboard.html."""
import json
from pathlib import Path

HERE = Path(__file__).parent
records = (json.loads((HERE / "actuals.json").read_text())
           + json.loads((HERE / "results.json").read_text()))
data = json.dumps(records, separators=(",", ":")).replace("</", "<\\/")
html = (HERE / "dashboard_template.html").read_text().replace("__DATA__", data)
(HERE / "dashboard.html").write_text(html)
print(f"dashboard.html written ({len(html)/1024:.0f} KB)")
