"""Inject results.json into dashboard_template.html -> dashboard.html."""
import json
from pathlib import Path

HERE = Path(__file__).parent
data = json.dumps(json.loads((HERE / "results.json").read_text()),
                  separators=(",", ":")).replace("</", "<\\/")
html = (HERE / "dashboard_template.html").read_text().replace("__DATA__", data)
(HERE / "dashboard.html").write_text(html)
print(f"dashboard.html written ({len(html)/1024:.0f} KB)")
