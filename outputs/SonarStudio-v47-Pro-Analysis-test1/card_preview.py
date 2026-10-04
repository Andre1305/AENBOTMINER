"""Decrypt/render personal PDF only in RAM. No password command-line arguments."""
import json,sys,io,base64
from pathlib import Path
from private_pdf import read_secret

def inspect_card(job):
 from pypdf import PdfReader
 import pypdfium2
 secret=read_secret(job['key']);reader=PdfReader(job['file']);encrypted=reader.is_encrypted
 if not encrypted or not reader.decrypt(secret):raise ValueError('PDF pessoal criptografado inválido.')
 wrong=PdfReader(job['file']);assert wrong.decrypt('wrong-test-password')==0
 document=pypdfium2.PdfDocument(str(job['file']),password=secret);images=[]
 for page in document:
  bitmap=page.render(scale=1.25);buffer=io.BytesIO();image=bitmap.to_pil();image.save(buffer,format='PNG');images.append(base64.b64encode(buffer.getvalue()).decode('ascii'));image.close();bitmap.close();page.close()
 document.close()
 return {'encrypted':encrypted,'wrong_password_rejected':True,'pages':len(reader.pages),'text':[p.extract_text() for p in reader.pages],'images':images,'attachments':list(reader.attachments)}

if __name__=='__main__':
 try:print(json.dumps(inspect_card(json.loads(sys.stdin.buffer.read().decode('utf-8')))))
 except Exception as error:print(str(error),file=sys.stderr);sys.exit(1)
