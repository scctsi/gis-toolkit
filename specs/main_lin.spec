# -*- mode: python ; coding: utf-8 -*-
block_cipher = None
import glob, os
rasterio_imports_paths = glob.glob(r'//opt//hostedtoolcache//Python//3.10.2//x64//lib//python3.10//site-packages//rasterio//*py')
rasterio_imports = ['rasterio._shim', 'rasterio.control', 'rasterio.rpc', 'rasterio.sample', 'rasterio.crs', 'rasterio.vrt']

for item in rasterio_imports_paths:
    current_module_filename = os.path.split(item)[-1]
    current_module_filename = 'rasterio.'+current_module_filename.replace('.py', '')
    rasterio_imports.append(current_module_filename)



a = Analysis(['../main.py'],
             pathex=['venv\\Lib\\site-packages'],
             binaries=[],
             datas=[],
             hiddenimports=rasterio_imports,
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='enhance_lin',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None )
