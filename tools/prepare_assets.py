"""Download public sonar weights; validate exact tested model digests."""
from pathlib import Path
import argparse,urllib.request,hashlib,json
MODELS=[('gv-yolo12','https://huggingface.co/PINGEcosystem/gv-yolo12/resolve/main/weights.onnx','a5f76feff2ca683c4ff4738c3b27ac979489d8ad92e81373301ff1646d93f998'),('sonarvision-v6','https://huggingface.co/Dinoman1221/sonarvision-yolov8-esi-v6/resolve/be0ab3954df54fa677449eaee5a7defdcfeae917/yolo_esi_v6_fp32.onnx','33a620674479e6251a3fb487e2b6a47dca777e3d3dd51d49c77fec6b2043ba72')]
# GhostVision digest is taken from the distributed provenance file.
def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as stream:
  for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
 return h.hexdigest()
def download(url,path,expected=None):
 path.parent.mkdir(parents=True,exist_ok=True)
 if path.is_file() and (expected is None or sha(path)==expected):return
 temporary=path.with_suffix(path.suffix+'.partial')
 with urllib.request.urlopen(url,timeout=180) as response,temporary.open('wb') as out:
  for block in iter(lambda:response.read(1024*1024),b''):out.write(block)
 if expected and sha(temporary)!=expected:raise ValueError('Model checksum mismatch; refusing untested replacement: '+url)
 temporary.replace(path)
def prepare(folder):
 folder=Path(folder)
 for name,url,digest in MODELS:
  provenance=folder/'models'/name/'provenance.json'
  if name=='gv-yolo12':digest=json.loads(provenance.read_text())['sha256']
  download(url,folder/'models'/name/'weights.onnx',digest)
 download('https://raw.githubusercontent.com/google/fonts/main/ofl/notosans/NotoSans%5Bwdth,wght%5D.ttf',folder/'assets/NotoSans.ttf')
 if (folder/'OFL.txt').exists():(folder/'assets/OFL.txt').write_bytes((folder/'OFL.txt').read_bytes())
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--folder',required=True);prepare(parser.parse_args().folder)
