# -*- mode: python ; coding: utf-8 -*-
import os
import sys

# 資源清單
datas_list = []

app_name = 'OpendataUA-macos-onefile' if sys.platform == 'darwin' else 'OpendataUA-onefile'

# 動態檢查圖示檔案並設定參數
icon_param = None
if sys.platform == 'darwin':  # macOS
    if os.path.exists('lioil.icns'):
        datas_list.insert(0, ('lioil.icns', '.'))
        icon_param = 'lioil.icns'
else:  # Windows
    if os.path.exists('lioil.ico'):
        datas_list.insert(0, ('lioil.ico', '.'))
        icon_param = 'lioil.ico'

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=datas_list,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name=app_name,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon_param, # 使用修正後的圖示變數
)