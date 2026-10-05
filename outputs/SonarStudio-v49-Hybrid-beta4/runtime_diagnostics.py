"""Windows child-process diagnostics; never infer memory exhaustion from a DLL code."""
import os

def child_environment(values=None):
 values=dict(os.environ if values is None else values)
 if os.name!='nt':return values
 result={}
 for key,value in values.items():result[key.upper()]=value
 return result

def exit_explanation(returncode):
 code=int(returncode)&0xffffffff
 if code==0xc06d007f:return '0xC06D007F: procedimento não encontrado em DLL carregada sob demanda; verificar DLLs e importações, não presumir falta de RAM.'
 if code==0xc06d007e:return '0xC06D007E: DLL carregada sob demanda não encontrada; verificar runtime e PATH.'
 return f'Código do processo: 0x{code:08X}.'
