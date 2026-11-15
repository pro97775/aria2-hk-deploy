# heroku-deploy

Minimal Heroku (container stack) loader for the mltb-only-aria2 worker. The
image contains only the bootstrap files; on boot `update.py` pulls the full bot
from your private upstream repository and `start.sh` runs it.

## 1. Build the base image

Deploy the sibling `cloud-base-image` repository first (GitHub Actions or
`build-base-image.sh`) and note the tag, e.g. `yourusername/cloud-base:latest`.

## 2. Point this repo at it

Edit the first line of `Dockerfile`:

```dockerfile
FROM yourusername/cloud-base:latest
```

## 3. Create the Heroku app

```bash
heroku create <app-name> --stack container
heroku config:set \
  BOT_TOKEN=... \
  DATABASE_URL=... \
  DATABASE_NAME=mltb \
  UPSTREAM_REPO=https://github.com/<you>/<private-upstream>.git \
  UPSTREAM_BRANCH=main
```

`UPSTREAM_REPO` must be reachable from the container. For a private repository
use a token URL (`https://<token>@github.com/<you>/<repo>.git`) or make the
repository public.

## 4. Deploy

```bash
git init -b main
git add .
git commit -m "Initial commit"
heroku git:remote -a <app-name>
git push heroku main
heroku ps:scale web=1
```

## Runtime notes

- `PORT` is injected by Heroku; the bot binds gunicorn to it automatically.
- `services.sh` writes all aria2 RPC/transfer options into a `0600`
  `.svc_rpc.conf` and starts the renamed binary (`blitzfetcher`), so no worker
  flags are visible in the process list.
- `/opt/media-tools` keeps `ffmpeg`/`ffprobe` discoverable for yt-dlp.
- The container filesystem is ephemeral; anything that must survive a restart
  belongs in `DATABASE_URL` or the upstream repository.
