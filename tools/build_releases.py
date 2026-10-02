"""Build download packages without private recordings, maps or local telemetry."""
from pathlib import Path
import json,hashlib,zipfile,shutil,compileall
from prepare_assets import prepare
root=Path(__file__).resolve().parents[1];manifest=json.loads((root/'sonarstudio-versions.json').read_text());dist=root/'dist';dist.mkdir(exist_ok=True)
for item in manifest['versions']:
 folder=root/item['folder'];prepare(folder);assert compileall.compile_dir(folder,quiet=1)
 package=dist/('SonarStudio-'+item['version']+'-windows.zip')
 files=[]
 for name in ['Instalar-SonarStudio.ps1','sonarstudio-runtime.toml','sonarstudio-versions.json','README.md']:
  files.append((root/name,name))
 for p in (root/'tools').glob('*.py'):files.append((p,str(p.relative_to(root))))
 for p in folder.rglob('*'):
  if p.is_file() and '__pycache__' not in p.parts:files.append((p,str(p.relative_to(root))))
 with zipfile.ZipFile(package,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
  for path,name in sorted(files):archive.write(path,name)
 with zipfile.ZipFile(package) as archive:assert archive.testzip() is None
 digest=hashlib.sha256(package.read_bytes()).hexdigest();(dist/(package.name+'.sha256')).write_text(digest+'  '+package.name+'\n')
 print(package.name,digest,flush=True)
