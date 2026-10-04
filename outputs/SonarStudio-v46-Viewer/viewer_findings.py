from contextlib import contextmanager
"""Personal SQLite findings. Manual tags are not model detections."""
from pathlib import Path
import sqlite3,json,uuid,time

class Findings:
 def __init__(self,path):
  self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
  with self.connection() as db:db.execute('CREATE TABLE IF NOT EXISTS findings(id TEXT PRIMARY KEY, source TEXT NOT NULL, ping INTEGER NOT NULL, tag TEXT NOT NULL, created REAL NOT NULL, metadata TEXT NOT NULL)')
 @contextmanager
 def connection(self):
  db=sqlite3.connect(self.path,timeout=5);db.execute('PRAGMA journal_mode=WAL')
  try:
   with db:yield db
  finally:db.close()
 def add(self,source,ping,tag,metadata):
  identity=uuid.uuid4().hex
  with self.connection() as db:db.execute('INSERT INTO findings VALUES (?,?,?,?,?,?)',(identity,str(source),int(ping),tag,time.time(),json.dumps(metadata,ensure_ascii=False,allow_nan=False)))
  return identity
 def list(self,source):
  with self.connection() as db:rows=db.execute('SELECT id,ping,tag,created,metadata FROM findings WHERE source=? ORDER BY ping',(str(source),)).fetchall()
  return [dict(id=r[0],ping=r[1],tag=r[2],created=r[3],metadata=json.loads(r[4])) for r in rows]
 def remove(self,identity):
  with self.connection() as db:db.execute('DELETE FROM findings WHERE id=?',(identity,))
