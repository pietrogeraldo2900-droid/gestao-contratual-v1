from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from main import process_text_file


def main():
    if len(sys.argv) < 2:
        print('Uso: python processar_pasta_txt.py pasta_com_txt')
        raise SystemExit(1)
    folder = Path(sys.argv[1])
    if not folder.exists() or not folder.is_dir():
        print('Pasta inválida:', folder)
        raise SystemExit(1)
    txts = sorted(folder.glob('*.txt'))
    if not txts:
        print('Nenhum arquivo .txt encontrado em', folder)
        raise SystemExit(1)
    total = 0
    for txt in txts:
        output_dir = folder / f'saida_{txt.stem}'
        process_text_file(txt, output_dir)
        print('Processado:', txt.name, '->', output_dir.name)
        total += 1
    print(f'Total processado: {total}')


if __name__ == '__main__':
    main()
