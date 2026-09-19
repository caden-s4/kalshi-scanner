# Deploying the Kalshi scanner on a fresh Ubuntu 24.04 droplet

This is a line-by-line runbook: from a brand-new DigitalOcean droplet with an
attached block-storage volume, to a recorder running unattended under systemd,
with hourly compaction, retention, and a free-space monitor.

Target box: Ubuntu 24.04 LTS, 1 vCPU / 1 GB RAM, plus a separate block-storage
volume for data. Run everything below as `root` (or with `sudo`) unless a step
says `su - kalshi`. Values you must change are in `<angle brackets>`.

Conventions used throughout:

| Thing            | Value                                |
|------------------|--------------------------------------|
| Service user     | `kalshi`                             |
| Code directory   | `/home/kalshi/kalshi-scanner`        |
| Virtualenv       | `/home/kalshi/kalshi-scanner/.venv`  |
| Data volume      | `/mnt/kalshi`                        |
| `KALSHI_DATA_DIR`| `/mnt/kalshi/data`                   |

---

## 1. System packages

```bash
apt-get update
apt-get install -y python3 python3-venv python3-pip git rsync ufw
python3 --version   # expect 3.12.x on Ubuntu 24.04 (project requires >= 3.11)
```

## 2. Create the non-root service user

```bash
adduser --system --group --home /home/kalshi --shell /bin/bash kalshi
# --system gives a locked-password account; the recorder never needs to log in.
```

## 3. Mount the block-storage volume at /mnt/kalshi

Identify the volume device (DigitalOcean volumes appear as
`/dev/disk/by-id/scsi-0DO_Volume_<name>`):

```bash
ls -l /dev/disk/by-id/ | grep -i volume
```

If the volume is brand new and has **no** filesystem yet, create one (this
ERASES the volume — skip if it already holds data):

```bash
mkfs.ext4 -F /dev/disk/by-id/scsi-0DO_Volume_<name>
```

Mount it now and persist it in `/etc/fstab` so it comes back after reboot:

```bash
mkdir -p /mnt/kalshi

# Get the stable filesystem UUID:
blkid /dev/disk/by-id/scsi-0DO_Volume_<name>
# -> /dev/sda: UUID="xxxxxxxx-...." TYPE="ext4"

# Append an fstab entry (use the UUID from above). `nofail` keeps the box
# bootable if the volume is ever detached; `x-systemd.device-timeout` bounds
# the wait so boot does not hang.
echo 'UUID=<uuid>  /mnt/kalshi  ext4  defaults,nofail,x-systemd.device-timeout=30s  0  2' >> /etc/fstab

systemctl daemon-reload
mount -a
findmnt /mnt/kalshi        # confirm it is mounted
```

Create the data directory on the volume and give it to the service user:

```bash
mkdir -p /mnt/kalshi/data
chown -R kalshi:kalshi /mnt/kalshi/data
```

## 4. Get the code

```bash
su - kalshi
git clone <your-repo-url> /home/kalshi/kalshi-scanner
cd /home/kalshi/kalshi-scanner
```

(If you deploy by copying files instead of git, `rsync` the project into
`/home/kalshi/kalshi-scanner` and make sure it is owned by `kalshi:kalshi`.)

## 5. Python environment

Still as `kalshi`, in `/home/kalshi/kalshi-scanner`:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

## 6. Credentials and configuration

The recorder reads config from a `.env` file in the code directory and an RSA
private key (`.pem`). Create a locked-down secrets directory:

```bash
mkdir -p /home/kalshi/kalshi-scanner/secrets
chmod 700 /home/kalshi/kalshi-scanner/secrets
```

Copy your Kalshi RSA private key into it (from your workstation):

```bash
# run this on your LOCAL machine:
scp ./kalshi-private-key.pem kalshi@<droplet-ip>:/home/kalshi/kalshi-scanner/secrets/kalshi.pem
```

Back on the droplet, lock the key down (the loader refuses a world-readable key
only informally — 600 is required practice):

```bash
chmod 600 /home/kalshi/kalshi-scanner/secrets/kalshi.pem
chown kalshi:kalshi /home/kalshi/kalshi-scanner/secrets/kalshi.pem
```

Write `.env` in the code directory (as `kalshi`):

```bash
cat > /home/kalshi/kalshi-scanner/.env <<'EOF'
KALSHI_KEY_ID=<your-key-id-uuid>
KALSHI_PEM_PATH=/home/kalshi/kalshi-scanner/secrets/kalshi.pem
KALSHI_DATA_DIR=/mnt/kalshi/data
# Optional; defaults shown:
# KALSHI_CHANNELS=ticker,trade
# KALSHI_TICKER_PREFIXES=
# MIN_FREE_GB_START=5
# MIN_FREE_GB_HALT=2
# MIN_FREE_GB_WARN=10
# RETENTION_MIN_AGE_DAYS=7
EOF
chmod 600 /home/kalshi/kalshi-scanner/.env
```

Smoke-test the configuration before involving systemd:

```bash
cd /home/kalshi/kalshi-scanner
.venv/bin/python -c "from config import CONFIG; print('OK', CONFIG.data_dir, CONFIG.key_id[:8])"
```

## 7. Firewall — SSH only

```bash
# run as root
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH          # inbound 22/tcp only
ufw --force enable
ufw status verbose
```

The recorder only makes **outbound** WSS connections to Kalshi, so no inbound
port beyond SSH is required.

## 8. Install the systemd units

The unit files in `deploy/` assume the paths in the table above. If you changed
any, edit the units before copying. Then, as `root`:

```bash
cd /home/kalshi/kalshi-scanner
cp deploy/kalshi-recorder.service   /etc/systemd/system/
cp deploy/kalshi-compactor.service  /etc/systemd/system/
cp deploy/kalshi-compactor.timer    /etc/systemd/system/
cp deploy/kalshi-freespace.service  /etc/systemd/system/
cp deploy/kalshi-freespace.timer    /etc/systemd/system/

systemctl daemon-reload

# Validate the units before enabling them (should print no warnings):
systemd-analyze verify /etc/systemd/system/kalshi-*.service /etc/systemd/system/kalshi-*.timer
```

Enable and start the recorder, and the two timers:

```bash
systemctl enable --now kalshi-recorder.service
systemctl enable --now kalshi-compactor.timer
systemctl enable --now kalshi-freespace.timer
```

The compactor/freespace **services** are triggered by their timers — you do not
enable them directly.

## 9. Check status and read logs

```bash
# Is the recorder up?
systemctl status kalshi-recorder.service

# Live recorder logs (stats line every 10s, rotations, reconnects):
journalctl -u kalshi-recorder.service -f

# When did the timers last run / when next?
systemctl list-timers 'kalshi-*'

# Last compaction + retention run:
journalctl -u kalshi-compactor.service -n 100 --no-pager

# Free-space monitor history (grep for the warning):
journalctl -u kalshi-freespace.service --no-pager | grep -E 'LOW DISK|disk OK'
```

Run a compaction/retention pass by hand (does not wait for the timer):

```bash
systemctl start kalshi-compactor.service
```

Preview retention without deleting anything:

```bash
su - kalshi -c 'cd /home/kalshi/kalshi-scanner && .venv/bin/python retention.py --dry-run'
```

## 10. What happens on failure (by design)

- **Recorder crashes** (network blip, unexpected error): systemd restarts it
  with backoff (5s, doubling up to 300s). Coverage gaps are recorded in the data
  as `type=gap` rows.
- **Disk fills up**: the recorder halts *cleanly* (flushing the final zstd frame)
  when free space drops below `MIN_FREE_GB_HALT`. `run_recorder.py` detects the
  low-disk condition and exits **75**; `RestartPreventExitStatus=75` stops
  systemd from restarting it into a full disk. Fix disk space, then
  `systemctl start kalshi-recorder.service`.
- **Volume detaches**: `RequiresMountsFor=/mnt/kalshi` stops the recorder rather
  than writing to the wrong disk.
- **Compaction reconciliation fails** for a file: `kalshi-compactor.service`
  fails, retention is **not** run that hour (nothing is deleted), and the failure
  is visible in `systemctl status kalshi-compactor.service`.

## 11. Pulling data off the box (from your workstation)

See `README.md` for full flags. Quick version, run **locally** (needs `rsync`
and SSH access to the droplet):

```bash
cd kalshi-scanner
VPS_HOST=kalshi@<droplet-ip> LOCAL_DATA_DIR=./data ./deploy/sync_pull.sh 2026-09-15
# ... verify passes ... then, only if you want to reclaim VPS space:
VPS_HOST=kalshi@<droplet-ip> LOCAL_DATA_DIR=./data ./deploy/sync_purge_remote.sh 2026-09-15 --confirm
```
