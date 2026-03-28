#!/usr/bin/env bash

set -euo pipefail

project_dir=""
config_path="config/config.yaml"
client_id=""

usage() {
  cat <<'EOF'
Usage: ./scripts/open-local-cluster.sh --project-dir <path> [--config-path <path>] [--client-id <id>]

Opens 4 replica terminals and 1 client terminal for a local DepChain cluster.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      project_dir="${2:-}"
      shift 2
      ;;
    --config-path)
      config_path="${2:-}"
      shift 2
      ;;
    --client-id)
      client_id="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$project_dir" ]]; then
  echo "--project-dir is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "$client_id" ]]; then
  echo "--client-id is required" >&2
  usage >&2
  exit 1
fi

project_dir="$(cd "$project_dir" && pwd)"

detect_terminal() {
  local candidates=(
    x-terminal-emulator
    gnome-terminal
    konsole
    xfce4-terminal
    xterm
  )

  local terminal
  for terminal in "${candidates[@]}"; do
    if command -v "$terminal" >/dev/null 2>&1; then
      printf '%s\n' "$terminal"
      return 0
    fi
  done

  return 1
}

launch_terminal() {
  local terminal="$1"
  local title="$2"
  local command="$3"

  case "$terminal" in
    x-terminal-emulator)
      "$terminal" -T "$title" -e bash -lc "$command; exec bash" &
      ;;
    gnome-terminal)
      "$terminal" --title="$title" -- bash -lc "$command; exec bash" &
      ;;
    konsole)
      "$terminal" --new-tab -p tabtitle="$title" -e bash -lc "$command; exec bash" &
      ;;
    xfce4-terminal)
      "$terminal" --title="$title" --hold -e "bash -lc '$command; exec bash'" &
      ;;
    xterm)
      "$terminal" -T "$title" -e bash -lc "$command; exec bash" &
      ;;
    *)
      echo "Unsupported terminal launcher: $terminal" >&2
      exit 1
      ;;
  esac
}

terminal="$(detect_terminal || true)"
if [[ -z "$terminal" ]]; then
  echo "Could not find a supported terminal emulator." >&2
  echo "Supported: x-terminal-emulator, gnome-terminal, konsole, xfce4-terminal, xterm" >&2
  exit 1
fi

server_ids=(server1 server2 server3 server4)
for server_id in "${server_ids[@]}"; do
  server_args="$server_id $config_path"
  server_command="cd '$project_dir' && echo \"Running: mvn -q exec:java@server -Dexec.args=\\\"$server_args\\\"\" && mvn -q exec:java@server \"-Dexec.args=$server_args\""
  launch_terminal "$terminal" "depchain-$server_id" "$server_command"
done

client_args="--config $config_path --client-id $client_id"
client_command="cd '$project_dir' && echo \"Running: mvn -q exec:java@client -Dexec.args=\\\"$client_args\\\"\" && mvn -q exec:java@client \"-Dexec.args=$client_args\""
launch_terminal "$terminal" "depchain-client-$client_id" "$client_command"
