#!/bin/bash
ARIA2C=${1:-blitzfetcher}

tracker_list=$(curl -Ns https://ngosang.github.io/trackerslist/trackers_all_http.txt | awk '$0' | tr '\n\n' ',')

cat <<CONF > .svc_rpc.conf
allow-overwrite=true
auto-file-renaming=true
bt-enable-lpd=true
bt-detach-seed-only=true
bt-remove-unselected-file=true
bt-tracker=[$tracker_list]
bt-max-peers=100
enable-rpc=true
rpc-max-request-size=64M
max-connection-per-server=4
max-concurrent-downloads=8
split=4
seed-ratio=1.0
continue=true
disk-cache=40M
force-save=true
min-split-size=10M
follow-torrent=mem
check-certificate=false
optimize-concurrent-downloads=true
http-accept-gzip=true
max-file-not-found=0
max-tries=20
peer-id-prefix=-qB4520-
reuse-uri=true
content-disposition-default-utf8=true
user-agent=Wget/1.12
peer-agent=qBittorrent/4.5.2
quiet=true
summary-interval=0
max-upload-limit=1K
CONF

chmod 600 .svc_rpc.conf
$ARIA2C --conf-path=.svc_rpc.conf --daemon=true
