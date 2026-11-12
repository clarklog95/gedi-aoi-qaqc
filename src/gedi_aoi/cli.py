"""Portable stage runner for the user-configured GEDI workflow."""
import argparse,datetime,json,os,pathlib,subprocess,sys,urllib.request
from contextlib import closing
from .selection import COLLECTIONS,DEFAULT_ATTRIBUTES,select
from .prepare import prepare

def validate(config):
    products=config['products']
    if not products or len(set(products))!=len(products) or any(p not in COLLECTIONS for p in products):raise ValueError('Choose a nonempty unique subset of L2A, L2B, L4A, L4C')
    if config.get('workers',4)<1 or config.get('batch_size',100000)<1:raise ValueError('workers and batch_size must be positive')
    dates=[datetime.datetime.fromisoformat(config[k].replace('Z','+00:00')) for k in ['start','end']]
    if any(d.tzinfo is None for d in dates) or dates[0]>dates[1]:raise ValueError('Use ordered timezone-aware UTC start/end dates')
    for p in products:
        from .selection import requested
        requested(config,p)

def stage(module,root,*args):
    env=dict(os.environ,GEDI_PROJECT_ROOT=str(root))
    subprocess.run([sys.executable,'-m','gedi_aoi.'+module,*args],env=env,check=True)

def capabilities(root,config):
    for p in config['products']:
        url='https://harmony.earthdata.nasa.gov/capabilities?collectionId='+config['collections'][p]
        with urllib.request.urlopen(url,timeout=90) as r:cap=json.load(r)
        selected=select(config,p,cap)
        (root/'metadata'/(p+'_harmony_capabilities.json')).write_text(json.dumps(cap,indent=2))
        print(p,len(selected),'beam-expanded fields validated')

def verify_manifest(root):
    import sqlite3
    with closing(sqlite3.connect(root/'metadata/full_download/manifest.sqlite')) as db:
        rows=db.execute('SELECT status,count(*) FROM jobs GROUP BY status').fetchall()
    if not rows:raise RuntimeError('No granules found; check AOI/date settings')
    if any(status not in ['complete','empty'] for status,n in rows):raise RuntimeError('Unresolved jobs: '+str(rows)+'. Run download again or inspect manifest; QA/QC has not started.')

def execute(config,argv=None):
    parser=argparse.ArgumentParser(description='GEDI polygon AOI download, QA/QC and verified Parquet')
    parser.add_argument('command',choices=['prepare','capabilities','download','progress','qaqc','combine','combine-progress','all'])
    args=parser.parse_args(argv);validate(config);root=pathlib.Path(config['project_root']).resolve();command=args.command
    if command in ['prepare','all'] and not (root/'project.json').exists():prepare(config)
    if not (root/'project.json').exists():raise RuntimeError('Run prepare first')
    saved=json.loads((root/'project.json').read_text())
    if saved!=config:raise RuntimeError('Configuration changed from project.json. Use the original settings to resume or choose a new output directory.')
    policy=json.loads(pathlib.Path(__file__).with_name('qc_policy.json').read_text());(root/'qa_steps1_5/reports').mkdir(parents=True,exist_ok=True);(root/'qa_steps1_5/reports/QC_POLICY.json').write_text(json.dumps(policy,indent=2))
    if command in ['capabilities','all']:capabilities(root,config)
    if command in ['download','all']:
        if not all((root/'metadata'/(p+'_harmony_capabilities.json')).exists() for p in config['products']):capabilities(root,config)
        stage('workflow.download',root);verify_manifest(root)
    if command=='progress':stage('workflow.download',root,'--progress')
    if command in ['qaqc','all']:
        verify_manifest(root);stage('workflow.index',root);stage('workflow.audit',root,'base');stage('workflow.native_flags',root);stage('workflow.audit',root,'finish')
    if command in ['combine','all']:stage('combine',root,'--project-root',str(root),'--batch-size',str(config.get('batch_size',100000)))
    if command=='combine-progress':stage('progress',root,'--project-root',str(root))

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=pathlib.Path,required=True);parser.add_argument('command');args=parser.parse_args()
    execute(json.loads(args.config.read_text()),[args.command])
