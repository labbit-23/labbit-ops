#!/usr/bin/env bash
set -Eeuo pipefail

# Create a restorable, secret-free backup of every Git repository under the
# Labit workspace. A bundle preserves committed history/all refs; a binary
# patch and filtered untracked archive preserve local work not in Git.

WORKSPACE="${WORKSPACE:-/home/sdrc/projects/labit}"
OUTPUT_ROOT="${OUTPUT_ROOT:-${WORKSPACE}/labit-ops/local-backups}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUTPUT_DIR="${OUTPUT_ROOT}/${RUN_ID}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1" >&2; exit 1; }; }
need git
need tar
need sha256sum
need find

mkdir -p "${OUTPUT_DIR}/repos"
manifest="${OUTPUT_DIR}/MANIFEST.tsv"
printf 'name\tpath\thead\tbranch\torigin\tdirty_entries\tbundle\tpatch\tuntracked\n' > "$manifest"

is_safe_untracked() {
  local path="$1"
  case "/$path" in
    */.env|*/.env.local|*/.env.production|*/.env.development|*/config.ini|*/services.ini|*/report_sender.json|*/mwl_worker_*.json) return 1 ;;
    */node_modules/*|*/.next/*|*/.venv/*|*/venv/*|*/__pycache__/*|*/target/*|*/build_cache/*|*/reports/*|*/.pytest_cache/*) return 1 ;;
    *.log|*.pid|*.sqlite|*.sqlite3|*.tsbuildinfo) return 1 ;;
  esac
  return 0
}

while IFS= read -r git_dir; do
  repo="${git_dir%/.git}"
  if ! head="$(git -C "$repo" rev-parse --verify HEAD 2>/dev/null)"; then
    continue
  fi

  rel="${repo#${WORKSPACE}/}"
  [[ "$repo" == "$WORKSPACE" ]] && rel="workspace-root"
  name="${rel//\//__}"
  branch="$(git -C "$repo" branch --show-current 2>/dev/null || true)"
  origin="$(git -C "$repo" config --get remote.origin.url 2>/dev/null || true)"
  origin="$(printf '%s' "$origin" | sed -E 's#(https?://)[^/@]+(:[^/@]*)?@#\1REDACTED@#')"
  dirty="$(git -C "$repo" status --porcelain=v1 | wc -l | tr -d ' ')"

  bundle="repos/${name}.bundle"
  patch="-"
  untracked="-"
  git -C "$repo" bundle create "${OUTPUT_DIR}/${bundle}" --all
  git -C "$repo" bundle verify "${OUTPUT_DIR}/${bundle}" >/dev/null

  if ! git -C "$repo" diff --quiet HEAD -- || ! git -C "$repo" diff --cached --quiet HEAD --; then
    patch="repos/${name}.worktree.patch"
    git -C "$repo" diff --binary HEAD -- > "${OUTPUT_DIR}/${patch}"
  fi

  untracked_list="$(mktemp)"
  while IFS= read -r -d '' path; do
    if is_safe_untracked "$path"; then
      printf '%s\0' "$path" >> "$untracked_list"
    else
      printf 'Excluded possible secret/generated untracked path: %s/%s\n' "$rel" "$path" >&2
    fi
  done < <(git -C "$repo" ls-files --others --exclude-standard -z)

  if [[ -s "$untracked_list" ]]; then
    untracked="repos/${name}.untracked.tar"
    tar -C "$repo" --null -T "$untracked_list" -cf "${OUTPUT_DIR}/${untracked}"
  fi
  rm -f "$untracked_list"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$repo" "$head" "$branch" "$origin" "$dirty" "$bundle" "$patch" "$untracked" >> "$manifest"
done < <(find "$WORKSPACE" -mindepth 1 -maxdepth 3 -type d -name .git -print | sort)

(
  cd "$OUTPUT_DIR"
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)

echo "Repository recovery set created: $OUTPUT_DIR"
echo "Verify with: (cd '$OUTPUT_DIR' && sha256sum -c SHA256SUMS)"
