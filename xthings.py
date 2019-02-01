import os
import json
import time

import click

import firebase_admin
import firebase_admin.auth
import firebase_admin.credentials
import firebase_admin.firestore



DEFAULT_LOCAL='.'



class FireStore_Collection:
  def __init__(self,cpath):
    self.path=cpath
    self.cred=firebase_admin.credentials.Certificate('../224.secrets/firebase.secrets.json')
    self.app=firebase_admin.initialize_app(self.cred)
    self.db=firebase_admin.firestore.client()
    self.collection=self.db.collection(self.path)

  def close(self):
    pass
#    self.collection.close()
#    self.db.close()
#    self.app.close()
#    self.cred.close()
    time.sleep(.3)

  def remoteobjects(self):
    return self.collection.get()

  def doc_fetch_data(self,doc):
    return doc.to_dict()

  def doc_delete(self,id):
    self.doc_create(id).delete()


  def doc_create(self,name,js=None):
    doc=self.collection.document(name)
    if js is not None: doc.set(js)
    return doc



class LocalTree:

  def __init__(self,localdir,dbpath):
    self.dir=localdir
    self.dbpath=dbpath
    #
    self.ldir=localdir+'/'+self.dbpath+'/'
    self.larchive=localdir+'/'+self.dbpath+'.archive/'
    if not self.ldir.endswith('/'): self.ldir=self.ldir+'/'
    if not self.larchive.endswith('/'): self.larchive=self.larchive+'/'
    if not os.path.exists(self.ldir): os.mkdir(self.ldir)
    if not os.path.exists(self.larchive): os.mkdir(self.larchive)
    #
    self.localfiles=os.listdir(self.ldir)
    for f in self.localfiles[:]:
      if f.endswith('.json'):
        self.localfiles.remove(f)
        self.localfiles.append(f[:-5])
    self.localfiles.sort()
    #

  def debug(self):
    print('# ltree.set ldir',self.ldir)
    print('# ltree.set larc',self.larchive)

  def json_diff(self,a,b):
    rv=[]
    for k in sorted(a.keys()):
      if k not in b: rv.append( ['--',k,a[k]] )
      elif a[k]!=b[k]: rv.append( ['!=',[k,a[k],b[k]]])
    for k in sorted(b.keys()):
      if k not in a: rv.append( ['++',k,b[k]])
    return rv

  def json_to_local(self,jsondata):
    return json.dumps(jsondata, sort_keys=True, indent=2)

  def filearchive(self,id):
    return '%s%s'%(self.larchive,id)
  def filelocal(self,id):
    return '%s%s'%(self.ldir,id)

  def write(self,path,data):
    print('  save.remote',path)
    open( path ,'w').write(data)
  def read(self,path):
    return open( path ,'r').read()
  def archive(self,lid):
    lp=self.filelocal(lid)
    la=self.filearchive(lid)
    print('  localarchive',[lid,lp,la])
    os.rename(lp,la)


  def pullremote(self,db,args={}):
    doit=args.get('doit')
    DEBUG=args.get('debug')
    if DEBUG:
      print("# DOIT status is",doit)
      print("# DEBUG status is",DEBUG)
      print('# --------- starting remote pull ---------')
    ndocs=0
    remotefiles=[]
    for doc in db.remoteobjects():
      id=doc.id
      rdata=db.doc_fetch_data(doc)
      rjson=self.json_to_local( rdata )
      if id in self.localfiles:
        ljson=self.read(self.filelocal(id+'.json'))
        ldata=json.loads(ljson)
        if ldata!=rdata:
          if DEBUG: print(u'{}     ## check failed, pulling remote {}'.format(id,self.json_diff(ldata,rdata)))
          if doit:
            self.archive(id+'.json')
            self.write( self.filelocal(id+'.json'), rjson )
        else:
          if DEBUG: print(u'## OK : {}'.format(id))
      else:
        if DEBUG: print(u'{}     ## new remote file {}'.format(id,[rjson]))
        if doit:
          self.write( self.filelocal(id+'.json'), rjson )
      remotefiles.append( id )
      ndocs+=1
    if DEBUG:
      print('# --------- remote pulled ---------')
      print('# checking ndocs',ndocs,len(remotefiles))
    if ndocs!=len(remotefiles): raise Exception
    if DEBUG:
      print('# --------- sanity check done ---------')
    for lid in self.localfiles:
#      print('texting',lid in remotefiles,[lid,remotefiles])
      if lid not in remotefiles:
        if DEBUG: print(u'{}     ## local not on remote, archiving'.format(lid))
        if doit:
          self.archive(lid+'.json')
    if DEBUG:
      print('# --------- local cleanup done ---------')


  def pushlocal(self,db,doit,args={}):
    doit=args.get('doit')
    DEBUG=args.get('debug')
    if DEBUG:
      print("# DOIT status is",doit)
      print("# DEBUG status is",DEBUG)
      print('# --------- starting remote only check ---------')
    ndocs=0
    remotefiles=[]
    for doc in db.remoteobjects():
      if doc.id not in self.localfiles:
        if DEBUG: print('{}    ## remote not on local !!!'.format(doc.id))
        if doit:
          db.doc_delete(doc.id)
      remotefiles.append( doc.id )

    if DEBUG:
      print('# --------- starting diffs to remote ---------')
    for id in self.localfiles:
      ljson=self.read(self.filelocal(id+'.json'))
      ldata=json.loads(ljson)
      if id in remotefiles:
        doc=db.doc_create(id).get()
        rdata=db.doc_fetch_data(doc)
        rjson=self.json_to_local( rdata )
        if ldata!=rdata:
          if DEBUG: print(u'{}    ## check failed, pushing local {}'.format(id,self.json_diff(rdata,ldata)))
          if doit:
            db.doc_create(id,ldata)
        else:
          if DEBUG: print(u'## OK : {}'.format(id))
      else:
        if DEBUG: print(u'{}    ## pushing new file {}'.format(id,[ljson]))
        if doit:
          db.doc_create(id,ldata)
      ndocs+=1
    if DEBUG:
      print('# --------- done ---------')
      print('##### checking ndocs',ndocs,len(remotefiles))
      print('# --------- done ---------')




#print()
#print("get a single doc")
#doc=db.collection('things').document('test')
#print(doc.get().to_dict())
#
#print()
#print("set a single doc")
#doc2=db.collection('things').document('frompy')
#doc2.set({'setfrom':'python','monster':'yeah'})
#
#print()
#print("set a single doc")
#doc2=db.collection('things').document('frompy')
#doc2.set({'anotherfield':'done!'},merge=True)
#


@click.group()
def cli():
  """Syncer to Firebase

  Allows pulling and pushing to a firestore repo from a local tree
  """
  pass

@cli.command()
@click.option('--local','-l',type=click.Path(exists=True,file_okay=False),default=DEFAULT_LOCAL,help="directory to hold collection directory (ie, final path==[local]/[collection]/")
@click.option('--app','-a',default="firebase.secrets.json",help="firebase config secret file")
@click.option('--collection','-c',type=click.STRING,default="things",help="firebase collection to work on")
@click.option('-y/-n','doit',default=False)
@click.option('--debug/--no-debug','debug',default=True)
def rpull(local,app,collection,doit,debug):
  '''sync all  --  remote firestore >> local
  '''
  if debug: click.echo('# using local {} and app {} and coll {}'.format(local,app,collection))
  fstore=FireStore_Collection(collection)
  lt=LocalTree( local, fstore.path )
  if debug: lt.debug()
  lt.pullremote(fstore,{'doit':doit,'debug':debug})
  if not doit:
     click.echo("### Rerun with -y to perform")
     click.echo("### Rerun with -y to perform")
     click.echo("### Rerun with -y to perform")
  fstore.close()

@cli.command()
@click.option('--local','-l',type=click.Path(exists=True,file_okay=False),default=DEFAULT_LOCAL,help="directory to hold collection directory (ie, final path==[local]/[collection]/")
@click.option('--app','-a',default="firebase.secrets.json",help="firebase config secret file")
@click.option('--collection','-c',type=click.STRING,default="things",help="firebase collection to work on")
@click.option('-y/-n','doit',default=False)
@click.option('--debug/--no-debug','debug',default=True)
def lpush(local,app,collection,doit,debug):
  '''sync all  --  local >> remote firestore
  '''
  if debug: click.echo('# using local {} and app {} and coll {}'.format(local,app,collection))
  fstore=FireStore_Collection(collection)
  lt=LocalTree( local, fstore.path )
  if debug: lt.debug()
  lt.pushlocal(fstore,doit,{'doit':doit,'debug':debug})
  if not doit:
     click.echo("### Rerun with -y to perform")
     click.echo("### Rerun with -y to perform")
     click.echo("### Rerun with -y to perform")


@cli.command()
@click.option('--app','-a',default="firebase.secrets.json",help="firebase config secret file")
@click.option('--collection','-c',type=click.STRING,default="things",help="firebase collection to work on")
def rls(app,collection):
  '''list remote firestore objects
  '''
  click.echo('# using and app {} and coll {}'.format(app,collection))
  fstore=FireStore_Collection(collection)
  for doc in fstore.remoteobjects():
    click.echo( doc.id )


@cli.command()
@click.option('--app','-a',default="firebase.secrets.json",help="firebase config secret file")
@click.option('--collection','-c',type=click.STRING,default="things",help="firebase collection to work on")
@click.argument('ids',nargs=-1)
def rrm(app,collection,ids):
  '''remove from remote firestore objects[s]
  '''
  click.echo('using and app {} and coll {} to nuke {}'.format(app,collection,ids))
  fstore=FireStore_Collection(collection)
  for id in ids:
    fstore.doc_delete(id)


@cli.command()
@click.option('--app','-a',default="firebase.secrets.json",help="firebase config secret file")
@click.option('--collection','-c',type=click.STRING,default="things",help="firebase collection to work on")
@click.argument('files',type=click.Path(exists=True,dir_okay=False),nargs=-1)
def pushfile(app,collection,files):
  '''sync specific file[s] -- local >> remote firestore
  '''
  click.echo('using app {} and coll {} to push {}'.format(app,collection,files))
  fstore=FireStore_Collection(collection)
  for f in files:
    id=f.split( os.path.sep )[-1]
    if id.endswith('.json'): id=id[:-5]
    js=json.load( open(f) )
    click.echo('  push {} {}'.format(id,js))
    fstore.doc_create(id,js)


if __name__=="__main__": cli()



