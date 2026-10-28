# Deployment guide — ETL platform on Azure

Everything about how this project is deployed, how to ship a change, the
everyday commands, how to rebuild the server from zero, and fixes for every
problem hit during the first deployment.

> **No secrets live in this file or in git.** Passwords and keys are only in the
> `.env` file on the VM (and your local `.env`). Never commit `.env`.

---

## 1. What runs where

```
                 ┌──────────────────── Azure VM  vm-etl-prod (South India) ─────────────────────┐
 Internet ──443──►  caddy (HTTPS) ──► etl-api            upload API (FastAPI)                   │
 Internet ─8443──►  caddy (HTTPS) ──► airflow-webserver  Airflow UI                             │
                 │                    airflow-scheduler  runs the pipelines ──────────────────────┼──► MySQL server
                 │                    postgres           Airflow's own bookkeeping only           │    (naturodb, modernkitchendb,
                 │                    GitHub runner      auto-deploys every push to main          │     ullasnirantaradb, hangyodb,
                 └────────────────────────────────────────────────────────────────────────────────┘     pbisalesanalysisdb, masterdb)
                                                     uploaded files ──► Azure Blob Storage (retaildata2026)
```

Your **business data** lives in **MySQL**. The Postgres container only stores
Airflow's run history, task states and the Airflow login users.

### Links

| | Local (your PC) | Deployed (Azure) |
|---|---|---|
| Airflow UI | `http://localhost:8080` | **https://etl-hangyo.southindia.cloudapp.azure.com:8443** |
| API docs (Swagger) | `http://localhost:8000/docs` | **https://etl-hangyo.southindia.cloudapp.azure.com/docs** |
| Upload endpoint | `http://localhost:8000/api/v1/etl/upload/file-upload` | **https://etl-hangyo.southindia.cloudapp.azure.com/api/v1/etl/upload/file-upload** |
| Airflow login | `airflow` / `airflow` | `admin` / value of `AIRFLOW_ADMIN_PASSWORD` in the VM's `.env` |
| Runs on | your PC, only while it's on | the VM, 24/7 |

See the Airflow password again (on the VM): `grep AIRFLOW_ADMIN_PASSWORD ~/etl/.env`

### Pipelines (Airflow DAGs)

| DAG | Starts | Source → destination |
|---|---|---|
| `hangyo_blinkit_qcom`, `hangyo_instamart_qcom`, `hangyo_zepto_qcom` | **every night at 01:00 IST** | pbisalesanalysisdb → hangyodb |
| `naturo_outlet_master`, `naturo_product_master` | when a file is uploaded | upload → naturodb |
| `modernkt_outlet_master`, `modernkt_product_master` | when a file is uploaded | upload → modernkitchendb |
| `ulsnirt_outlet_master`, `ulsnirt_product_master`, `ulsnirt_secondary_sales` | when a file is uploaded | upload → ullasnirantaradb |

⚠️ **Only one environment should run pipelines.** Local and Azure use the *same*
MySQL. Keep the local stack stopped unless you are testing:
`docker compose -f D:\etl\docker\docker-compose.yml down`

---

## 2. Shipping a change (CI/CD) — the normal way

Every push to **`main`** is checked and deployed automatically by
`.github/workflows/deploy.yml`:

```
git push (main) ──► ① Check code (GitHub's machine)
                        - every Python file compiles
                        - docker/docker-compose.prod.yml is valid
                        ❌ any error → STOP, nothing is deployed
                    ② Deploy to Azure VM (self-hosted runner on the VM)
                        - git pull
                        - docker compose up -d --build
                        - restart etl-api + airflow-scheduler
```

### Steps (PowerShell on your PC)

```powershell
cd D:\etl
git checkout main                 # the VM only follows main
git add .
git commit -m "describe the change"
git push
```

Then watch it: **https://github.com/shreyasukco/etl-pipeline/actions** —
both boxes (*Check code* → *Deploy to Azure VM*) should turn ✅ in 2–3 minutes.

### Working on a branch

```powershell
git checkout -b feature/my-change      # work here; pushes do NOT deploy
git push -u origin feature/my-change
# when ready:
git checkout main
git merge feature/my-change
git push                               # ← this deploys
```

### Rules

- **Don't push to `main` around 01:00 IST** — a deploy restarts the scheduler and
  interrupts a running nightly pipeline (re-running is safe, but avoid it).
- **Never edit code files directly on the VM.** The deploy does `git pull --ff-only`,
  which fails if the VM has local edits. Change code on your PC and push.
- **Keep the repository private.** A self-hosted runner on a public repo would let
  anyone run code on your server.
- Manual re-deploy: GitHub → **Actions** → **CI/CD** → **Run workflow**.
- A change to **`.env`** is *not* deployed by CI/CD — see §4.

---

## 3. Logging into the VM

From PowerShell on your PC (the prompt shows `PS C:\...`):

```powershell
ssh -o ServerAliveInterval=60 -i $HOME\.ssh\vm-etl-prod_key.pem azureuser@etl-hangyo.southindia.cloudapp.azure.com
```

You're in when the prompt becomes `azureuser@vm-etl-prod:~$`.

| Prompt starts with | You are on | Use it for |
|---|---|---|
| `PS C:\...` / `PS D:\...` | your **PC** | `ssh`, `scp`, `git` in `D:\etl` |
| `azureuser@vm-etl-prod` | the **VM** | `dc`, `git pull`, `grep`, `ls` — **never `ssh` from here** |

`ServerAliveInterval=60` stops Azure from dropping an idle connection.
Leave the VM with `exit`.

---

## 4. Everyday commands (on the VM)

`dc` is a shortcut (in `~/.bashrc`) for
`docker compose --env-file ~/etl/.env -f ~/etl/docker/docker-compose.prod.yml`.

| Task | Command |
|---|---|
| Status of all containers | `cd ~/etl && dc ps` |
| Follow pipeline logs | `dc logs -f airflow-scheduler` (Ctrl+C to stop) |
| API / HTTPS logs | `dc logs --tail 50 etl-api` · `dc logs --tail 50 caddy` |
| ETL log file | `tail -f ~/etl/logs/etl_pipeline.log` |
| List DAGs | `dc exec airflow-scheduler airflow dags list` |
| Restart everything | `cd ~/etl && dc restart` |
| Stop / start everything | `dc down` · `dc up -d` |
| Manual deploy (no CI/CD) | `cd ~/etl && git pull && dc up -d --build && dc restart etl-api airflow-scheduler` |
| Test MySQL from Airflow | see below |
| Memory / disk | `free -h` · `df -h /` |
| Airflow's Postgres console | `dc exec postgres psql -U airflow -d airflow` (`\dt`, `\q`) |
| CI/CD runner status | `cd ~/actions-runner && sudo ./svc.sh status` |

Test that the pipelines reach every database:

```bash
dc exec -w /opt/airflow airflow-scheduler python -c "from core.queries.db_connection import get_engine; from sqlalchemy import text; [print(c, get_engine(c).connect().execute(text('SELECT DATABASE()')).scalar()) for c in ('NATURO','MODERNKT','ULSNIRT','HANGYO')]"
```

### Changing `.env` on the VM

```bash
nano ~/etl/.env          # edit, Ctrl+O Enter to save, Ctrl+X to exit
chmod 644 ~/etl/.env     # keep it readable by the containers
cd ~/etl && dc up -d && dc restart etl-api airflow-scheduler
```

The VM's `.env` is separate from your PC's `D:\etl\.env` — changing one does not
change the other.

---

## 5. The server (Azure resources)

| Item | Value |
|---|---|
| Subscription | Azure subscription 1 |
| Resource group | `vm-etl-prod_group` |
| VM | `vm-etl-prod` — Ubuntu 24.04, **Standard_B2als_v2** (2 vCPU, 4 GB RAM) + 4 GB swap, 64 GB Standard SSD, auto-shutdown **off** |
| Region | South India (same as storage account `retaildata2026` in `rg-retail-data`) |
| DNS name | `etl-hangyo.southindia.cloudapp.azure.com` |
| SSH key (on your PC) | `C:\Users\<you>\.ssh\vm-etl-prod_key.pem` — the only key; back it up, never share or commit it |
| VM user | `azureuser` |
| Code on VM | `~/etl` (clone of `shreyasukco/etl-pipeline`, `main`) |
| Secrets on VM | `~/etl/.env` (mode 644) |
| CI/CD runner | `~/actions-runner`, systemd service `actions.runner.shreyasukco-etl-pipeline.vm-etl-prod` |
| GitHub → VM access | deploy key `~/.ssh/etl_deploy` (GitHub repo → Settings → Deploy keys → `azure-vm`) |

### Firewall (NSG `vm-etl-prod-nsg`, inbound)

| Priority | Name | Port | Source | Why |
|---|---|---|---|---|
| 300 | HTTP | 80 | Any | HTTPS certificate + redirect |
| 320 | HTTPS | 443 | Any | upload API |
| 340 | SSH | 22 | **your IP only** | log in |
| 350 | Airflow-HTTPS-8443 | 8443 | Any | Airflow UI |

If SSH says **Connection timed out**, your internet IP changed: VM → Networking →
Network settings → rule **SSH** → Source **My IP address** → Save.

### Cost

The VM costs ~USD 0.053/hour (~USD 39/month running 24/7) plus a few dollars for
the disk and public IP. Check spend in **Cost Management**.

```powershell
az vm deallocate -g vm-etl-prod_group -n vm-etl-prod   # stop billing for compute (nightly runs won't happen)
az vm start      -g vm-etl-prod_group -n vm-etl-prod   # everything comes back up by itself
```

Delete everything (irreversible): `az group delete -n vm-etl-prod_group`

---

## 6. Upload API

```
POST https://etl-hangyo.southindia.cloudapp.azure.com/api/v1/etl/upload/file-upload
     ?client_code=<NATURO|MODERNKT|ULSNIRT>
     &file_type=<OUTLETDUMP|PRODUCT|SECONDARYDUMP>
     &ext_erp_rec_code=<ERP code>
Body: multipart/form-data, field "file" = the CSV/Excel file
```

| Client | ERP rec code | Pipelines |
|---|---|---|
| NATURO | `ERP10001` | outlet, product |
| MODERNKT | `ERPDZ1001` | outlet, product |
| ULSNIRT | `ERPNC1001` | outlet, product, secondary |

The response streams progress (`UPLOAD_RECEIVED → … → STORE_BLOB → ETL_TRIGGERED`)
and the matching DAG starts automatically.

```powershell
curl.exe -N -X POST "https://etl-hangyo.southindia.cloudapp.azure.com/api/v1/etl/upload/file-upload?client_code=NATURO&file_type=OUTLETDUMP&ext_erp_rec_code=ERP10001" -F "file=@D:\path\to\file.xlsx"
```

Other endpoints: `GET /health` · `GET /api/v1/etl/tasks?client_code=NATURO` ·
`GET /api/v1/etl/tasks/{task_id}` ·
`GET /api/v1/etl/upload/get-file-template?client_code=NATURO&file_type=OUTLETDUMP&download=true`

⚠️ The API has **no login** — anyone with the URL can upload and trigger loads
into the real MySQL. Add authentication before real production use.

---

## 7. Rebuilding the server from zero

Use this if the VM is deleted or you deploy to a new one. Allow ~1 hour.

### 7.1 Create the VM (Azure portal)

Portal → **Virtual machines** → **+ Create** → **Azure virtual machine**:

- **Basics:** subscription; resource group (new); name `vm-etl-prod`; region
  same as the storage account; *No infrastructure redundancy*; image
  **Ubuntu Server 24.04 LTS - x64 Gen2**; size **B2als_v2** or bigger (if a size
  says *NotAvailableForSubscription*, pick another 2 vCPU size); **SSH public key**,
  username `azureuser`, **Generate new key pair**, key name `vm-etl-prod_key`;
  inbound ports **HTTP 80, HTTPS 443, SSH 22**.
- **Disks:** 64 GiB, Standard SSD.
- **Networking:** defaults; tick *Delete public IP and NIC when VM is deleted*.
- **Management:** **auto-shutdown off** (nightly runs need the VM on).
- **Create** → **Download private key and create resource** (only chance!).

Then:
- VM **Overview** → **DNS name: Not configured** → label `etl-hangyo` → **Apply**.
- VM → **Networking** → **Network settings**: SSH rule → Source **My IP address**;
  **+ Create port rule → Inbound**: port `8443`, TCP, Allow, priority 350, name
  `Airflow-HTTPS-8443`.

### 7.2 Key on your PC (PowerShell)

```powershell
New-Item -ItemType Directory -Force $HOME\.ssh
Move-Item $HOME\Downloads\vm-etl-prod_key.pem $HOME\.ssh\
icacls $HOME\.ssh\vm-etl-prod_key.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
ssh -o ServerAliveInterval=60 -i $HOME\.ssh\vm-etl-prod_key.pem azureuser@etl-hangyo.southindia.cloudapp.azure.com
```

### 7.3 Prepare the VM (on the VM)

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER && exit        # then ssh in again
docker run --rm hello-world                  # "Hello from Docker!"

sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

nc -zv -w 5 <MYSQL_HOST> 3306                # must say "succeeded"
```

### 7.4 Code (on the VM)

```bash
ssh-keygen -t ed25519 -f ~/.ssh/etl_deploy -N ""
cat ~/.ssh/etl_deploy.pub
```

GitHub → repo → **Settings → Deploy keys → Add deploy key** → paste → **Add key**
(leave *Allow write access* unticked). Then:

```bash
GIT_SSH_COMMAND="ssh -i ~/.ssh/etl_deploy -o StrictHostKeyChecking=accept-new" git clone git@github.com:shreyasukco/etl-pipeline.git ~/etl
git -C ~/etl config core.sshCommand "ssh -i ~/.ssh/etl_deploy"
```

### 7.5 Secrets

On your **PC**:

```powershell
scp -i $HOME\.ssh\vm-etl-prod_key.pem D:\etl\.env azureuser@etl-hangyo.southindia.cloudapp.azure.com:~/etl/.env
```

On the **VM** — fix line endings/permissions, create the log folder, then
replace the local Airflow settings with production ones (keys and passwords are
generated on the VM; the last line prints the Airflow login — save it):

```bash
sed -i 's/\r$//' ~/etl/.env && chmod 644 ~/etl/.env
mkdir -p ~/etl/logs && sudo chown -R 50000:0 ~/etl/logs
sed -i '/^AIRFLOW_BASE_URL=/d;/^AIRFLOW_USERNAME=/d;/^AIRFLOW_PASSWORD=/d' ~/etl/.env
echo >> ~/etl/.env; PW=$(openssl rand -hex 12); printf '%s\n' 'AIRFLOW_BASE_URL="http://airflow-webserver:8080"' 'AIRFLOW_USERNAME=admin' "AIRFLOW_PASSWORD=$PW" 'AIRFLOW_ADMIN_USER=admin' "AIRFLOW_ADMIN_PASSWORD=$PW" "AIRFLOW_DB_PASSWORD=$(openssl rand -hex 16)" "AIRFLOW_FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')" "AIRFLOW_WEBSERVER_SECRET_KEY=$(openssl rand -hex 32)" 'API_DOMAIN=etl-hangyo.southindia.cloudapp.azure.com' >> ~/etl/.env; echo "Airflow login -> user: admin   password: $PW"
grep -E "^(AIRFLOW_|API_DOMAIN)" ~/etl/.env | cut -d= -f1      # must list 9 names
```

Production `.env` keys (besides the MySQL/Azure/client-DB keys copied from the PC):
`AIRFLOW_BASE_URL`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`, `AIRFLOW_ADMIN_USER`,
`AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_DB_PASSWORD` (letters/digits only),
`AIRFLOW_FERNET_KEY`, `AIRFLOW_WEBSERVER_SECRET_KEY`, `API_DOMAIN`.

### 7.6 Start (on the VM)

```bash
echo "alias dc='docker compose --env-file $HOME/etl/.env -f $HOME/etl/docker/docker-compose.prod.yml'" >> ~/.bashrc && source ~/.bashrc
cd ~/etl && dc up -d --build
dc ps                                                       # 5 containers Up; webserver + postgres healthy
dc exec airflow-scheduler airflow dags list                 # 10 DAGs
```

Open https://etl-hangyo.southindia.cloudapp.azure.com/docs and
https://etl-hangyo.southindia.cloudapp.azure.com:8443 (give Caddy 1–2 minutes to
get the certificate).

### 7.7 CI/CD runner (on the VM)

GitHub → repo → **Settings → Actions → Runners → New self-hosted runner** →
**Linux / x64**. Run its *Download* commands inside `~/actions-runner`, then its
`./config.sh --url ... --token ...` line (press **Enter** for all 4 questions).
**Don't** run `./run.sh`; install it as a service instead:

```bash
cd ~/actions-runner
sudo ./svc.sh install azureuser
sudo ./svc.sh start
```

The Runners page should show **vm-etl-prod ● Idle**. The registration token
expires after 1 hour.

---

## 8. Troubleshooting (all hit during the first deployment)

| Symptom | Cause | Fix |
|---|---|---|
| `Identity file /home/azureuser.sshvm-etl-prod_key.pem not accessible` | ran `ssh` **inside the VM** | you're already on the VM — skip `ssh`; Ctrl+C |
| `client_loop: send disconnect: Connection reset` | Azure dropped an idle SSH session | log in again with `-o ServerAliveInterval=60` |
| `ssh: connect ... port 22: Connection timed out` | your IP changed | NSG rule SSH → Source *My IP address* |
| `grep : The term 'grep' is not recognized` | ran a Linux command on the **PC** | log into the VM first |
| `PermissionError: [Errno 13] Permission denied: '.env'` | `.env` not readable by the container user (uid 50000) | `chmod 644 ~/etl/.env` |
| `Size ... NotAvailableForSubscription` (VM create) | trial subscription limits | choose another 2 vCPU size, or another region |
| API or Airflow URL won't load | Caddy still getting the certificate / port closed | wait 2 min; `dc logs --tail 30 caddy`; check NSG ports 80/443/8443 |
| `airflow-init` exits with error | special characters in `AIRFLOW_DB_PASSWORD` | letters and digits only |
| Upload stream ends with `ETL_TRIGGER_SKIPPED` | `AIRFLOW_USERNAME/PASSWORD` don't match the Airflow admin, or no DAG for that client/file type | fix `.env`, `dc up -d`; check §1 pipeline table |
| Pipeline can't reach MySQL | MySQL firewall doesn't allow the VM's IP | allow the VM public IP on 3306; test `nc -zv -w 5 <MYSQL_HOST> 3306` |
| Instamart load killed / very slow | out of memory (4 GB) | check `free -h`; resize the VM to 8 GB (portal → VM → Size) |
| CI/CD deploy step stuck "Waiting for a runner" | runner service stopped | `cd ~/actions-runner && sudo ./svc.sh status` / `start` |
| CI/CD deploy fails at `git pull --ff-only` | someone edited files on the VM | `cd ~/etl && git status`, then `git checkout -- .` (discards VM edits) and re-run |
| Local Airflow UI blank / `ERR_EMPTY_RESPONSE` (PC) | stale webserver pid / broken Docker Desktop mount | `docker exec etl_airflow sh -c "rm -f /opt/airflow/airflow-webserver*.pid"` then `docker restart etl_airflow` |

---

## 9. Security checklist

- [ ] `.env` and `vm-etl-prod_key.pem` are **never** committed (both are in `.gitignore` / outside the repo).
- [ ] Repository stays **private** (self-hosted runner).
- [ ] SSH allowed from your IP only.
- [ ] Rotate the MySQL password and the Azure Storage key if they were ever shared.
- [ ] The GitHub deploy key `azure-vm` is read/write — recreate it read-only for tighter security.
- [ ] The upload API has no authentication — add it before real production use.
