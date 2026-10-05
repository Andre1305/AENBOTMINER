"""Local account-bound evidence PDFs; password never written to disk/logs."""
from pathlib import Path
import ctypes,os
from ctypes import wintypes

def read_secret(key_path):
 class Blob(ctypes.Structure):
  _fields_=[('size',wintypes.DWORD),('data',ctypes.POINTER(ctypes.c_ubyte))]
 data=Path(key_path).read_bytes();buffer=(ctypes.c_ubyte*len(data)).from_buffer_copy(data);source=Blob(len(data),buffer);clear=Blob()
 crypt=ctypes.WinDLL('crypt32',use_last_error=True);crypt.CryptUnprotectData.argtypes=[ctypes.POINTER(Blob),ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(Blob)];crypt.CryptUnprotectData.restype=wintypes.BOOL
 if not crypt.CryptUnprotectData(ctypes.byref(source),None,None,None,None,1,ctypes.byref(clear)):raise OSError('Não foi possível abrir a chave no perfil Windows atual.')
 kernel=ctypes.WinDLL('kernel32');kernel.LocalFree.argtypes=[ctypes.c_void_p];kernel.LocalFree.restype=ctypes.c_void_p
 try:
  value=ctypes.string_at(clear.data,clear.size).decode('utf-8')
  if len(value)<40:raise ValueError('Chave pessoal inválida.')
  return value
 finally:
  ctypes.memset(clear.data,0,clear.size);kernel.LocalFree(clear.data)

def key_path():
 path=Path(__file__).resolve().parents[2]/'outputs/Documentacao-Privada-AEN-v44/chave.dpapi'
 if not path.is_file():raise FileNotFoundError('Chave pessoal DPAPI ausente. A ficha exige sua chave local de documentação.')
 return path

def pdf_python():
 path=Path(os.environ['USERPROFILE'])/'.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
 if not path.is_file():raise FileNotFoundError('Runtime local de PDF não encontrado.')
 return path

def preview_pdf(parent,path):
 import json,subprocess,base64
 from PySide6.QtGui import QPixmap
 from PySide6.QtWidgets import QDialog,QVBoxLayout,QScrollArea,QWidget,QLabel
 result=subprocess.run([str(pdf_python()),str(Path(__file__).parent/'card_preview.py')],input=json.dumps({'file':str(path),'key':str(key_path())}).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW,timeout=60)
 if result.returncode:raise ValueError(result.stderr.decode('utf-8',errors='replace')[-1000:])
 pages=json.loads(result.stdout)['images'];dialog=QDialog(parent);dialog.setWindowTitle('Ficha de alvo — uso pessoal');dialog.resize(880,850);layout=QVBoxLayout(dialog);scroll=QScrollArea();content=QWidget();items=QVBoxLayout(content)
 for page in pages:
  pixmap=QPixmap();pixmap.loadFromData(base64.b64decode(page));label=QLabel();label.setPixmap(pixmap);items.addWidget(label)
 scroll.setWidget(content);layout.addWidget(scroll);dialog.exec()
