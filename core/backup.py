"""
OPTIONEX — SQLite Backup Utility
Daily backup of optionex.db. Keeps last 30 days.
"""
import shutil, logging
from datetime import datetime, timedelta
from pathlib import Path
from config import DB_PATH, BACKUP_DIR

logger = logging.getLogger(__name__)

def run_backup() -> dict:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    if not DB_PATH.exists():
        return {"status": "skipped", "reason": "Database not found"}
    backup_name = f"optionex_{datetime.today().strftime('%Y%m%d')}.db"
    backup_path = BACKUP_DIR / backup_name
    try:
        shutil.copy2(DB_PATH, backup_path)
        _cleanup(30)
        return {"status": "ok", "backup_path": str(backup_path),
                "size_kb": round(backup_path.stat().st_size / 1024, 1)}
    except Exception as e:
        return {"status": "error", "error": str(e)}

def _cleanup(keep_days):
    cutoff = datetime.today() - timedelta(days=keep_days)
    for f in BACKUP_DIR.glob("optionex_*.db"):
        try:
            d = datetime.strptime(f.stem.replace("optionex_",""), "%Y%m%d")
            if d < cutoff: f.unlink()
        except (ValueError, OSError): pass

def list_backups() -> list:
    if not BACKUP_DIR.exists(): return []
    return [{"filename": f.name, "size_kb": round(f.stat().st_size/1024,1),
             "created_at": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")}
            for f in sorted(BACKUP_DIR.glob("optionex_*.db"), reverse=True)]
