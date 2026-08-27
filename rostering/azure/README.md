# Deploying the on-call board to Azure

This folder is a ready-to-deploy **Azure Static Web App**: the board UI plus
a small `/api/state` Function that keeps the shared state in Blob Storage
with ETag optimistic concurrency (a stale save gets a 409 and the client
reloads the latest board instead of overwriting anyone). Browsers refresh in
the background every 30s and on window focus, so GPs on different devices
see each other's saves.

```
azure/
  app/index.html               the board (built - do not edit directly)
  app/staticwebapp.config.json SWA config (node:20 API runtime)
  api/state/                   GET/PUT /api/state Azure Function
  build_azure_app.py           rebuilds app/index.html from ../availability_app.html
```

## One-time setup (~5 minutes, portal path)

1. **Create the Static Web App**
   Azure Portal → Create resource → *Static Web App* → Free plan.
   - Source: **GitHub** → repo `sluckyy/Clinical-Health-tracker`,
     branch `claude/gp-rostering-ed-obstetrics-wlsndd` (or `main` after merging)
   - Build presets: **Custom**
   - App location: `rostering/azure/app`
   - Api location: `rostering/azure/api`
   - Output location: *(leave empty)*

   Azure commits a GitHub Actions workflow to the branch; every push
   redeploys automatically. Your site appears at
   `https://<something>.azurestaticapps.net`.

2. **Give the API somewhere to keep the state**
   ```bash
   az storage account create -n <uniquename> -g <resource-group> -l australiaeast --sku Standard_LRS
   az storage account show-connection-string -n <uniquename> -g <resource-group> -o tsv
   az staticwebapp appsettings set -n <swa-name> \
     --setting-names STATE_STORAGE_CONNECTION="<paste the connection string>"
   ```
   (Or in the portal: Static Web App → *Environment variables* → add
   `STATE_STORAGE_CONNECTION`.) Without it, `/api/state` returns an error and
   the app falls back to per-browser mode with a visible badge.

That's it. Open the URL, sign in (GP PIN `1234`, admin `4321`), and the badge
should read **“Shared board (Azure)”**.

## CLI alternative

```bash
npx @azure/static-web-apps-cli deploy rostering/azure/app \
  --api-location rostering/azure/api \
  --deployment-token $(az staticwebapp secrets list -n <swa-name> --query properties.apiKey -o tsv) \
  --env production
```

## Updating the app

Edit `rostering/availability_app.html` (the single source of truth for the
UI and rules), then:

```bash
python3 rostering/azure/build_azure_app.py
git add rostering/azure/app/index.html && git commit && git push
```

## Before sharing beyond the demo

- The default `*.azurestaticapps.net` URL is public and the in-page PINs are
  demo-grade. For the Thursday demo, just don't circulate the URL. To lock it
  down properly, require sign-in with SWA's built-in auth by adding to
  `staticwebapp.config.json`:
  ```json
  "routes": [{ "route": "/*", "allowedRoles": ["authenticated"] }]
  ```
  and invite users under the Static Web App's *Role management* blade
  (supports Microsoft Entra ID - i.e. the LHN accounts - or GitHub).
- The shared state contains real staff names. Same rule: private URL for the
  demo, real auth before anything more.
