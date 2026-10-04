"""OpenGL color shader, bounded framebuffer; explicit CPU fallback."""
import numpy as np
from PySide6.QtGui import QOpenGLContext,QOffscreenSurface,QSurfaceFormat,QImage
from PySide6.QtOpenGL import QOpenGLShaderProgram,QOpenGLShader,QOpenGLTexture,QOpenGLFramebufferObject,QOpenGLBuffer
from image_processing import palette_rgb
from viewer_filters import colors

VERTEX='''#version 120
attribute vec2 position; varying vec2 uv;
void main(){ gl_Position=vec4(position,0.0,1.0); uv=vec2((position.x+1.0)*0.5,1.0-(position.y+1.0)*0.5); }
'''
FRAGMENT='''#version 120
uniform sampler2D source; uniform sampler2D original; uniform sampler2D lut;
uniform float blackLevel; uniform float whiteLevel; uniform float gammaValue;
uniform float splitPosition; uniform bool compare; varying vec2 uv;
void main(){ float value=(compare && uv.x<splitPosition)?texture2D(original,uv).r:texture2D(source,uv).r;
value=pow(clamp((value-blackLevel)/(whiteLevel-blackLevel),0.0,1.0),1.0/gammaValue);
gl_FragColor=vec4(texture2D(lut,vec2((value*255.0+0.5)/256.0,0.5)).rgb,1.0); }
'''

class ColorShader:
 def __init__(self):
  self.context=None;self.program=None;self.surface=None;self.textures=[];self.fbo=None;self.signature=None;self.palette=None;self.backend='CPU';self.reason='';self.renderer='unavailable'
  try:
   self.context=QOpenGLContext();fmt=QSurfaceFormat();fmt.setVersion(2,1);self.context.setFormat(fmt)
   if not self.context.create():raise RuntimeError('OpenGL context unavailable')
   self.surface=QOffscreenSurface();self.surface.setFormat(self.context.format());self.surface.create()
   if not self.context.makeCurrent(self.surface):raise RuntimeError('Offscreen OpenGL unavailable')
   self.functions=self.context.functions();self.functions.initializeOpenGLFunctions();renderer=self.functions.glGetString(0x1F01);self.renderer=renderer.decode(errors='replace') if isinstance(renderer,bytes) else str(renderer)
   self.program=QOpenGLShaderProgram()
   if not self.program.addShaderFromSourceCode(QOpenGLShader.ShaderTypeBit.Vertex,VERTEX) or not self.program.addShaderFromSourceCode(QOpenGLShader.ShaderTypeBit.Fragment,FRAGMENT) or not self.program.link():raise RuntimeError(self.program.log())
   self.vertices=np.array([[-1,-1],[1,-1],[-1,1],[1,1]],np.float32);self.buffer=QOpenGLBuffer();self.buffer.create();self.buffer.bind();self.buffer.allocate(self.vertices.tobytes(),self.vertices.nbytes);self.buffer.release();self.backend='OpenGL shader';self.context.doneCurrent()
  except Exception as error:self.reason=str(error);self.backend='CPU'
 def texture(self,array):
  a=np.ascontiguousarray(array,np.uint8);image=QImage(a.data,a.shape[1],a.shape[0],a.strides[0],QImage.Format.Format_Grayscale8).copy()
  t=QOpenGLTexture(image,QOpenGLTexture.MipMapGeneration.DontGenerateMipMaps);t.setMinMagFilters(QOpenGLTexture.Filter.Nearest,QOpenGLTexture.Filter.Nearest);t.setWrapMode(QOpenGLTexture.WrapMode.ClampToEdge);return t

 def render(self,treated,original,settings,compare=False,split=.5):
  if settings.white<=settings.black:raise ValueError('Nível branco deve superar o preto.')
  if max(treated.shape)>8192:return self.cpu(treated,original,settings,compare,split)
  if self.backend=='CPU':return self.cpu(treated,original,settings,compare,split)
  try:
   self.last_backend='OpenGL shader'
   if not self.context.makeCurrent(self.surface):raise RuntimeError('Lost OpenGL context')
   signature=(id(treated),id(original),treated.shape)
   if signature!=self.signature:
    for t in self.textures:t.destroy()
    self.textures=[self.texture(treated),self.texture(original)];self.signature=signature;self.retained=(treated,original)
    if self.fbo is not None:del self.fbo
    self.fbo=QOpenGLFramebufferObject(treated.shape[1],treated.shape[0]);self.fbo_size=treated.shape
   if self.palette!=settings.palette:
    if hasattr(self,'lut'):self.lut.destroy()
    rgba=np.dstack([np.round(palette_rgb(np.linspace(0,1,256),settings.palette)*255).astype('uint8')[None,:,:],np.full((1,256),255,np.uint8)])
    image=QImage(rgba.data,256,1,rgba.strides[0],QImage.Format.Format_RGBA8888).copy();self.lut=QOpenGLTexture(image,QOpenGLTexture.MipMapGeneration.DontGenerateMipMaps);self.lut.setMinMagFilters(QOpenGLTexture.Filter.Linear,QOpenGLTexture.Filter.Linear);self.lut.setWrapMode(QOpenGLTexture.WrapMode.ClampToEdge);self.palette=settings.palette
   if not self.fbo.isValid() or not self.fbo.bind():raise RuntimeError('Invalid OpenGL framebuffer')
   f=self.functions;f.glViewport(0,0,treated.shape[1],treated.shape[0]);self.program.bind()
   for unit,t in enumerate([*self.textures,self.lut]):t.bind(unit)
   for name,value in [('source',0),('original',1),('lut',2)]:self.functions.glUniform1i(self.program.uniformLocation(name),value)
   for name,value in [('blackLevel',float(settings.black/255)),('whiteLevel',float(settings.white/255)),('gammaValue',float(max(.1,settings.gamma))),('splitPosition',float(split))]:self.functions.glUniform1f(self.program.uniformLocation(name),value)
   self.functions.glUniform1i(self.program.uniformLocation('compare'),int(compare));location=self.program.attributeLocation('position');self.buffer.bind();self.program.enableAttributeArray(location);self.program.setAttributeBuffer(location,0x1406,0,2);f.glDrawArrays(0x0005,0,4);self.program.disableAttributeArray(location);self.buffer.release();self.program.release();image=self.fbo.toImage(True).convertToFormat(QImage.Format.Format_RGBA8888);self.fbo.release();self.context.doneCurrent();return image
  except Exception as error:self.reason=str(error);self.backend='CPU';return self.cpu(treated,original,settings,compare,split)
 def cpu(self,treated,original,settings,compare,split):
  self.last_backend='CPU'
  rgba=colors(treated,settings)
  if compare:
   cut=round(rgba.shape[1]*split);rgba[:,:cut]=colors(original[:,:cut],settings)
  return QImage(rgba.data,rgba.shape[1],rgba.shape[0],rgba.strides[0],QImage.Format.Format_RGBA8888).copy()
 def close(self):
  if self.context and self.surface and self.context.makeCurrent(self.surface):
   for t in self.textures:t.destroy()
   if hasattr(self,'lut'):self.lut.destroy()
   self.fbo=None;self.program=None;self.buffer.destroy() if hasattr(self,'buffer') else None;self.context.doneCurrent()

