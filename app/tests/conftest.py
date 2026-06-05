import sys
from pathlib import Path

# Adiciona o root do repositório ao sys.path para que `import app...` funcione
# Esta solução é usada apenas em testes para não alterar ambiente de produção.
ROOT = Path(__file__).resolve().parents[2]
APP_DIR = ROOT / "app"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
