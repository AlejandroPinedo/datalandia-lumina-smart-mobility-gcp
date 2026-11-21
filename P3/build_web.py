"""Compila React con Node y actualiza el consumidor servido por Flask y stdlib."""
from pathlib import Path
import os
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def build():
    subprocess.run(['npm.cmd' if os.name=='nt' else 'npm','run','build'],
                   cwd=ROOT/'frontend',check=True)
    return (ROOT/'Avance1_P1_Lumina_Ingesta_Storage/templates/p3_olap.html').read_text(encoding='utf-8')


if __name__ == '__main__':
    build()
    print('Consumidor Lumina actualizado.')
