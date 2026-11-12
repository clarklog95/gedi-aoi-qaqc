"""Shared science presets, NASA authentication and portable worker locking."""
import pathlib,os,json,sys,getpass,base64,urllib.request,urllib.parse,urllib.error
ROOT=pathlib.Path(os.environ['GEDI_PROJECT_ROOT']).resolve()
CONFIG=json.loads((ROOT/'project.json').read_text())
COLS = {'L2A': 'C3974616071-LPCLOUD', 'L2B': 'C3974616135-LPCLOUD', 'L4A': 'C4212593885-ORNL_CLOUD', 'L4C': 'C4302347663-ORNL_CLOUD'}
BEAMS = ['BEAM0000', 'BEAM0001', 'BEAM0010', 'BEAM0011', 'BEAM0101', 'BEAM0110', 'BEAM1000', 'BEAM1011']
class Redirect(urllib.request.HTTPRedirectHandler):

    def redirect_request(self, req, fp, code, msg, headers, url):
        new = super().redirect_request(req, fp, code, msg, headers, url)
        if new and urllib.parse.urlparse(req.full_url).hostname != urllib.parse.urlparse(url).hostname:
            new.remove_header('Authorization')
        return new
COLS={p:CONFIG.get('collections',COLS)[p] for p in CONFIG['products']}
# Include orbit flags at acquisition, avoiding the extra retrieval required by older exports.
def get_token():
    token=os.environ.get('EARTHDATA_TOKEN','').strip()
    if token:return token
    if not sys.stdin.isatty():raise RuntimeError('Set EARTHDATA_TOKEN or run interactively to sign in. Never commit credentials.')
    username=input('NASA Earthdata username: ').strip();password=getpass.getpass('NASA Earthdata password (not saved): ')
    basic=base64.b64encode((username+':'+password).encode()).decode()
    req=urllib.request.Request('https://urs.earthdata.nasa.gov/api/users/find_or_create_token',data=b'',headers={'Authorization':'Basic '+basic,'Accept':'application/json','Content-Type':'application/x-www-form-urlencoded'})
    try:
        with urllib.request.urlopen(req,timeout=60) as r:reply=json.load(r)
    except urllib.error.HTTPError as exc:raise RuntimeError('NASA authentication HTTP '+str(exc.code)) from None
    return reply['access_token']
TOKEN=None
def acquire_project_lock(path):
    guard=path.open('a+b');guard.seek(0);guard.write(b'1');guard.flush();guard.seek(0)
    try:
        if os.name=='nt':
            import msvcrt;msvcrt.locking(guard.fileno(),msvcrt.LK_NBLCK,1)
        else:
            import fcntl;fcntl.flock(guard.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except OSError:
        guard.close();raise RuntimeError('A download worker is already running') from None
    return guard
