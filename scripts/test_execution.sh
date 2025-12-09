#!/usr/bin/env bash
set -euo pipefail

echo "=============================================="
echo "🚀 NSF PIPELINE (NO API COLLECTOR)"
echo "=============================================="
echo "Started at: $(date)"
echo

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📂 Working directory: $SCRIPT_DIR"
echo

# ---------------------------------------------
# 1️⃣ COLLECTORS (SERIAL, NO API)
# ---------------------------------------------
echo "🔽 Running collectors (no API)..."

echo "➡️  collect_terminated.py"
python collect_terminated.py
echo "✅ collect_terminated done"
echo

echo "➡️  collect_cruz_list.py"
python collect_cruz_list.py
echo "✅ collect_cruz_list done"
echo

echo "➡️  collect_legislators.py"
python collect_legislators.py
echo "✅ collect_legislators done"
echo

echo "✅ All collectors completed"
echo

# ---------------------------------------------
# 2️⃣ FORMATTERS (SERIAL)
# ---------------------------------------------
echo "🧹 Running formatters..."

echo "➡️  format_awards.py"
python format_awards.py
echo "✅ format_awards done"
echo

echo "➡️  format_terminated.py"
python format_terminated.py
echo "✅ format_terminated done"
echo

echo "➡️  format_cruz_list.py"
python format_cruz_list.py
echo "✅ format_cruz_list done"
echo

echo "➡️  format_legislators.py"
python format_legislators.py
echo "✅ format_legislators done"
echo

echo "✅ All formatters completed"
echo

# ---------------------------------------------
# 3️⃣ TRANSFORMER
# ---------------------------------------------
echo "🔄 Running Mongo → Delta transformer..."

echo "➡️  data_transformer.py"
python data_transformer.py
echo "✅ Transformer done"
echo

# ---------------------------------------------
# ✅ END
# ---------------------------------------------
echo "=============================================="
echo "✅ PIPELINE FINISHED SUCCESSFULLY"
echo "Finished at: $(date)"
echo "=============================================="
